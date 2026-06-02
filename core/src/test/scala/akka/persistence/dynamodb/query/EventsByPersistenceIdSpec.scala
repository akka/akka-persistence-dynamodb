/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.query

import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.InstrumentationProvider
import akka.persistence.dynamodb.TestActors
import akka.persistence.dynamodb.TestActors.Persister
import akka.persistence.dynamodb.TestActors.Persister.PersistWithAck
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.dynamodb.TestInstrumentation
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.query.PersistenceQuery
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

object QueryInstrumentation {
  val qiFcn = "akka.persistence.dynamodb.TestInstrumentation"
  val config = s"""akka.persistence.dynamodb.instrumentation-class = "$qiFcn""""
}

object EventsByPersistenceIdSpec {
  def config: Config = ConfigFactory.parseString(QueryInstrumentation.config).withFallback(TestConfig.config)
}

class EventsByPersistenceIdSpec
    extends ScalaTestWithActorTestKit(EventsByPersistenceIdSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import akka.persistence.dynamodb.TestInstrumentationProtocol._

  override def typedSystem: ActorSystem[_] = system

  private val instrumentation = InstrumentationProvider(system).instrumentation.asInstanceOf[TestInstrumentation]

  private val query =
    PersistenceQuery(testKit.system).readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)

  override def beforeEach(): Unit = {
    instrumentation.resetProbe()
    super.beforeEach()
  }

  "CurrentEventsByPersistenceIdTypedQuery" should {
    "retrieve requested events" in {
      val entityType = nextEntityType()
      val pid = nextPersistenceId(entityType)
      val persister = testKit.spawn(Persister(pid))
      var ebpic = instrumentation.probe.expectMessageType[EventsByPersistenceIdCalled]
      ebpic.persistenceId shouldBe pid.id
      ebpic.fromSequenceNumber shouldBe 1
      ebpic.toSequenceNumber shouldBe Long.MaxValue
      ebpic.probe.expectNoMessage(30.millis) // no events
      instrumentation.probe.expectNoMessage(10.millis)

      val events = (1 to 20)
        .map { i =>
          val payload = s"e-$i"
          persister ! PersistWithAck(payload, system.ignoreRef)
          i -> payload
        }
        .map { case (i, payload) =>
          val wec = instrumentation.probe.expectMessageType[WriteEventsCalled]
          wec.persistenceId shouldBe pid.id
          wec.firstSequenceNr shouldBe i
          wec.count shouldBe 1
          // Discard the probe
          payload
        }

      val result1 =
        query.currentEventsByPersistenceIdTyped[String](pid.id, 0, Long.MaxValue).runWith(Sink.seq).futureValue
      result1.map(_.event) shouldBe events
      ebpic = instrumentation.probe.expectMessageType[EventsByPersistenceIdCalled]
      ebpic.persistenceId shouldBe pid.id
      ebpic.fromSequenceNumber shouldBe 0
      ebpic.toSequenceNumber shouldBe Long.MaxValue
      val queryProbe = ebpic.probe
      events.iterator.zipWithIndex.foreach { case (_, i) =>
        val receiveEvent = queryProbe.expectMessageType[QueryReceivedEvent]
        receiveEvent.persistenceId shouldBe pid.id
        receiveEvent.sequenceNumber shouldBe (i + 1)
        val eventProbe = receiveEvent.probe
        eventProbe.expectMessage(BeforeDeserializeEvent)
        eventProbe.expectMessage(AfterDeserializeEvent)
        eventProbe.expectNoMessage(10.millis)
      }
      queryProbe.expectNoMessage(10.millis)
      instrumentation.probe.expectNoMessage(10.millis)

      val result2 =
        query.currentEventsByPersistenceIdTyped[String](pid.id, 1, 5).runWith(Sink.seq).futureValue
      result2.map(_.event) shouldBe events.take(5)

      val result3 =
        query.currentEventsByPersistenceIdTyped[String](pid.id, 3, 7).runWith(Sink.seq).futureValue
      result3.head.event shouldBe "e-3"
      result3.last.event shouldBe "e-7"
      result3.map(_.event) shouldBe events.slice(2, 7)

      query
        .currentEventsByPersistenceIdTyped[String](nextPersistenceId(entityType).id, 1, 10)
        .runWith(Sink.seq)
        .futureValue
        .size shouldBe 0
    }

    "include metadata" in {
      val entityType = nextEntityType()
      val entityId = "entity-1"
      val persister = testKit.spawn(TestActors.replicatedEventSourcedPersister(entityType, entityId))
      val probe = testKit.createTestProbe[Done]()
      persister ! Persister.PersistWithAck("e-1", probe.ref)
      probe.expectMessage(Done)
      persister ! Persister.PersistWithAck("e-2", probe.ref)
      probe.expectMessage(Done)

      val result1 =
        query
          .currentEventsByPersistenceIdTyped[String](PersistenceId(entityType, entityId).id, 0, Long.MaxValue)
          .runWith(Sink.seq)
          .futureValue

      val env1 = result1.head
      env1.event shouldBe "e-1"
      val meta1 = env1.metadata[ReplicatedEventMetadata].get
      meta1.originReplica.id shouldBe "dc-1"
      meta1.originSequenceNr shouldBe 1L

      val env2 = result1(1)
      env2.event shouldBe "e-2"
      val meta2 = env2.metadata[ReplicatedEventMetadata].get
      meta2.originReplica.id shouldBe "dc-1"
      meta2.originSequenceNr shouldBe 2L
    }
  }

}
