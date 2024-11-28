/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.query

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.TestActors
import akka.persistence.dynamodb.TestActors.Persister
import akka.persistence.dynamodb.TestActors.Persister.PersistWithAck
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.query.PersistenceQuery
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import org.scalatest.wordspec.AnyWordSpecLike

object EventsByPersistenceIdSpec {

  def config: Config = TestConfig.config
}

class EventsByPersistenceIdSpec
    extends ScalaTestWithActorTestKit(EventsByPersistenceIdSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val query =
    PersistenceQuery(testKit.system).readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)

  "CurrentEventsByPersistenceIdTypedQuery" should {
    "retrieve requested events" in {
      val entityType = nextEntityType()
      val pid = nextPersistenceId(entityType)
      val persister = testKit.spawn(Persister(pid))
      val probe = testKit.createTestProbe[Done]()
      val events = (1 to 20).map { i =>
        val payload = s"e-$i"
        persister ! PersistWithAck(payload, probe.ref)
        probe.expectMessage(Done)
        payload
      }

      val result1 =
        query.currentEventsByPersistenceIdTyped[String](pid.id, 0, Long.MaxValue).runWith(Sink.seq).futureValue
      result1.map(_.event) shouldBe events

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
      val meta1 = env1.eventMetadata.get.asInstanceOf[ReplicatedEventMetadata]
      meta1.originReplica.id shouldBe "dc-1"
      meta1.originSequenceNr shouldBe 1L

      val env2 = result1(1)
      env2.event shouldBe "e-2"
      val meta2 = env2.eventMetadata.get.asInstanceOf[ReplicatedEventMetadata]
      meta2.originReplica.id shouldBe "dc-1"
      meta2.originSequenceNr shouldBe 2L
    }
  }

}
