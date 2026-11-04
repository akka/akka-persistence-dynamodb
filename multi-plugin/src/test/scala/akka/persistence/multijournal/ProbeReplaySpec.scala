/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

import akka.actor.ActorRef
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.JournalProtocol
import akka.persistence.Persistence
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.dynamodb.util.scaladsl.CreateTables
import akka.persistence.typed.PersistenceId
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

object ReviewProbeSpec {
  val Config: Config = ConfigFactory
    .parseString(s"""
    |akka.persistence.dynamodb.client.local.enabled = true
    |akka.persistence.dynamodb.journal.publish-events = off
    |akka.actor.testkit.typed.default-timeout = 10s
    |
    |review-cold = $${akka.persistence.dynamodb}
    |review-cold.journal.table = "review_cold"
    |review-hot = $${akka.persistence.dynamodb}
    |review-hot.journal.table = "review_hot"
    |
    |review-multi = $${akka.persistence.multi-journal}
    |review-multi.journals {
    |  0 = "review-cold.journal"
    |  1 = "review-hot.journal"
    |}
    |# same journals, separate per-pid cache, stands in for a restarted node
    |review-multi-fresh = $${review-multi}
    |review-multi-fresh2 = $${review-multi}
    |review-multi-fresh3 = $${review-multi}
    |review-multi-fresh4 = $${review-multi}
    |review-multi-fresh5 = $${review-multi}
    |review-multi-fresh6 = $${review-multi}
    |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()
}

class ReviewProbeSpec
    extends ScalaTestWithActorTestKit(ReviewProbeSpec.Config)
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with LogCapturing
    with TestData {
  import ReadTheWritesSpec.TestActor
  import TestActor._

  private def journal(id: String): ActorRef = Persistence(system.classicSystem).journalFor(id)

  private def getMetadata(journal: ActorRef, pid: String): Future[Map[Int, Long]] = {
    val promise = Promise[Map[Int, Long]]()
    journal ! MultiJournal.GetMetadata(pid, promise)
    promise.future
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Seq("review-cold", "review-hot").foreach { path =>
      val settings = DynamoDBSettings(system.settings.config.getConfig(path))
      val client = ClientProvider(system).clientFor(s"$path.client")
      Await.result(CreateTables.createJournalTable(system, settings, client, deleteIfExists = true), 10.seconds)
    }
  }

  private def writeCold2Hot4(): String = {
    val pid = PersistenceId("Review", nextPid()).id
    val state = createTestProbe[Vector[String]]()
    val cold = spawn(TestActor(pid, "review-cold.journal"))
    cold ! Persist(Seq("c1", "c2"), state.ref)
    state.receiveMessage()
    testKit.stop(cold)
    val multi = spawn(TestActor(pid, "review-multi"))
    multi ! Persist(Seq("h3", "h4", "h5", "h6"), state.ref)
    state.receiveMessage()
    testKit.stop(multi)
    pid
  }

  private def replay(journal: ActorRef, pid: String, from: Long, to: Long = Long.MaxValue): Vector[Any] = {
    val probe = createTestProbe[Any]()
    journal ! JournalProtocol.ReplayMessages(from, to, Long.MaxValue, pid, probe.ref.toClassic)
    var seen = Vector.empty[Any]
    var done = false
    while (!done) {
      val msg = probe.receiveMessage()
      seen :+= msg
      done = msg match {
        case _: JournalProtocol.ReplayedMessage => false
        case _                                  => true
      }
    }
    seen
  }

  private def show(msgs: Vector[Any]): String =
    msgs
      .map {
        case JournalProtocol.ReplayedMessage(r) => s"event(${r.sequenceNr})"
        case other                              => other.toString
      }
      .mkString(", ")

  private def delete(journal: ActorRef, pid: String, to: Long): Any = {
    val probe = createTestProbe[Any]()
    journal ! JournalProtocol.DeleteMessagesTo(pid, to, probe.ref.toClassic)
    probe.receiveMessage()
  }

  "review probes" should {
    "F1: recover after a delete that spans the journal boundary" in {
      val pid = writeCold2Hot4()
      delete(journal("review-multi"), pid, 4) shouldBe a[DeleteMessagesSuccess]

      val coldRead = replay(journal("review-cold.journal"), pid, 1)
      info(s"plain cold journal replay: ${show(coldRead)}")
      val multiRead = replay(journal("review-multi-fresh"), pid, 1)
      info(s"multi replay: ${show(multiRead)}")

      coldRead.last shouldBe a[JournalProtocol.RecoverySuccess]
      multiRead.last shouldBe a[JournalProtocol.RecoverySuccess]
    }

    "F2: keep cooler metadata when recovering from a snapshot at the latest seqNr" in {
      val pid = writeCold2Hot4()
      val fresh = journal("review-multi-fresh")

      // what a persistent actor does when the snapshot covers everything
      val read = replay(fresh, pid, 7)
      info(s"replay from 7: ${show(read)}")
      read.last shouldBe a[JournalProtocol.RecoverySuccess]

      val metadata = getMetadata(fresh, pid).futureValue
      info(s"metadata: $metadata")

      val deleted = delete(fresh, pid, 1)
      info(s"delete to 1: $deleted")
      val cold = replay(journal("review-cold.journal"), pid, 1)
      info(s"cold after delete to 1: ${show(cold)}")

      metadata shouldBe Map(0 -> 2L)
      cold.collect { case JournalProtocol.ReplayedMessage(r) => r.sequenceNr } shouldBe Vector(2L)
    }

    "F2b: survive a misrouted delete and keep metadata across restarts" in {
      val pid = writeCold2Hot4()
      val first = journal("review-multi-fresh3")
      replay(first, pid, 7).last shouldBe a[JournalProtocol.RecoverySuccess]
      delete(first, pid, 1)

      val read = replay(journal("review-multi-fresh4"), pid, 1)
      info(s"full replay after misrouted delete: ${show(read)}")
      read.last shouldBe a[JournalProtocol.RecoverySuccess]
    }

    "F2c: durable metadata after writing following a snapshot-at-latest recovery" in {
      val pid = writeCold2Hot4()
      val first = journal("review-multi-fresh5")
      replay(first, pid, 7).last shouldBe a[JournalProtocol.RecoverySuccess]
      val writer = createTestProbe[Any]()
      val repr = akka.persistence.PersistentRepr(
        "h7",
        sequenceNr = 7L,
        persistenceId = pid,
        writerUuid = java.util.UUID.randomUUID().toString)
      first ! JournalProtocol.WriteMessages(Seq(akka.persistence.AtomicWrite(repr)), writer.ref.toClassic, 1)
      writer.receiveMessage() shouldBe JournalProtocol.WriteMessagesSuccessful

      // new cache: metadata comes from the newest hot event only
      val restarted = journal("review-multi-fresh6")
      delete(restarted, pid, 1)
      val metadata = getMetadata(restarted, pid).futureValue
      info(s"metadata after restart: $metadata")
      metadata shouldBe Map(0 -> 2L)
    }

    "F3: answer replay-only-last when all events are deleted" in {
      val pid = writeCold2Hot4()
      delete(journal("review-multi"), pid, 6) shouldBe a[DeleteMessagesSuccess]

      val read = replay(journal("review-multi-fresh2"), pid, -1)
      info(s"replay-only-last: ${show(read)}")
      read.last shouldBe JournalProtocol.RecoverySuccess(6L)
    }
  }
}
