/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.journal

import java.time.Instant

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.TestActors.Persister
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

class PersistTimestampSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  private val serialization = SerializationExtension(system)
  case class Item(pid: String, seqNr: Long, timestamp: Instant, event: String)

  private def selectAllItems(): IndexedSeq[Item] = {
    import akka.persistence.dynamodb.internal.JournalAttributes._
    val req = ScanRequest.builder
      .tableName(settings.journalTable)
      .consistentRead(true)
      .build()

    Source
      .fromPublisher(client.scanPaginator(req))
      .mapConcat { response =>
        response.items().asScala.map { item =>
          val event = serialization
            .deserialize(
              item.get(EventPayload).b().asByteArray(),
              item.get(EventSerId).n().toInt,
              item.get(EventSerManifest).s())
            .get
            .asInstanceOf[String]
          Item(
            pid = item.get(Pid).s(),
            seqNr = item.get(SeqNr).n().toLong,
            timestamp = InstantFactory.fromEpochMicros(item.get(Timestamp).n().toLong),
            event)
        }
      }
      .runWith(Sink.seq)
      .futureValue
      .toVector
  }

  "Persist timestamp" should {

    "be increasing for events stored in same transaction" in {
      val numberOfEntities = 20
      val entityType = nextEntityType()

      val entities = (0 until numberOfEntities).map { n =>
        val persistenceId = PersistenceId(entityType, s"p$n")
        spawn(Persister(persistenceId), s"p$n")
      }

      (1 to 100).foreach { n =>
        val p = n % numberOfEntities
        // mix some persist 1 and persist 3 events
        val event = s"e$p-$n"
        if (n % 5 == 0) {
          // same event stored 3 times
          entities(p) ! Persister.PersistAll((0 until 3).map(_ => event).toList)
        } else {
          entities(p) ! Persister.Persist(event)
        }
      }

      val pingProbe = createTestProbe[Done]()
      entities.foreach { ref =>
        ref ! Persister.Ping(pingProbe.ref)
      }
      pingProbe.receiveMessages(entities.size, 20.seconds)

      val rows = selectAllItems()

      rows.groupBy(_.event).foreach { case (_, rowsByUniqueEvent) =>
        withClue(s"pid [${rowsByUniqueEvent.head.pid}]: ") {
          rowsByUniqueEvent.map(_.timestamp).sliding(2).foreach {
            case Seq(a, b) => a.isBefore(b) shouldBe true
            case _         => ()
          }
        }
      }

      val rowOrdering: Ordering[Item] = Ordering.fromLessThan[Item] { (a, b) =>
        if (a eq b) false
        else if (a.timestamp != b.timestamp) a.timestamp.compareTo(b.timestamp) < 0
        else {
          if (a.pid == b.pid && a.seqNr != b.seqNr)
            throw new IllegalStateException(s"Unexpected same timestamp for several events of [$a.pid]")
          a.pid.compareTo(b.pid) < 0
        }
      }

      rows.groupBy(_.pid).foreach { case (_, rowsByPid) =>
        withClue(s"pid [${rowsByPid.head.pid}]: ") {
          rowsByPid.sortBy(_.seqNr) shouldBe rowsByPid.sorted(rowOrdering)
        }
      }
    }

  }
}
