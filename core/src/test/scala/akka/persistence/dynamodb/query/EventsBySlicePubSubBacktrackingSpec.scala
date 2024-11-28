/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.query

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.internal.pubsub.TopicImpl
import akka.persistence.dynamodb.TestActors
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.dynamodb.internal.PubSub
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.typed.EventEnvelope
import akka.stream.testkit.scaladsl.TestSink

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import scala.concurrent.duration._
import akka.persistence.dynamodb.internal.EnvelopeOrigin

object EventsBySlicePubSubBacktrackingSpec {
  def config: Config = ConfigFactory
    .parseString("""
      akka.persistence.dynamodb {
        journal.publish-events = on
        query {
          refresh-interval = 300 ms
          
          # Ensure pubsub arrives first
          behind-current-time = 2s
  
          backtracking {
            behind-current-time = 3500ms
            window = 6s
          }
        }
      }
      akka.actor.testkit.typed.filter-leeway = 20 seconds
    """)
    .withFallback(TestConfig.config)
}

class EventsBySlicePubSubBacktrackingSpec
    extends ScalaTestWithActorTestKit(EventsBySlicePubSubBacktrackingSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val query =
    PersistenceQuery(testKit.system).readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)

  private class Setup {
    val entityType = nextEntityType()
    val persistenceId = nextPersistenceId(entityType)
    val slice = query.sliceForPersistenceId(persistenceId.id)
    val persister = spawn(TestActors.Persister(persistenceId))
    val probe = createTestProbe[Done]()
    val sink = TestSink[EventEnvelope[String]]()

    val Persist = TestActors.Persister.Persist
    val PersistWithAck = TestActors.Persister.PersistWithAck
  }

  "EventsBySlices pub-sub with backtracking enabled" should {
    "publish events" in new Setup {
      val result =
        query
          .eventsBySlices[String](entityType, slice, slice, NoOffset)
          .runWith(sink)
          .request(1000)

      val topicStatsProbe = createTestProbe[TopicImpl.TopicStats]()
      eventually {
        PubSub(typedSystem).eventTopic[String](this.entityType, slice) ! TopicImpl.GetTopicStats(topicStatsProbe.ref)
        topicStatsProbe.receiveMessage().localSubscriberCount shouldBe 1
      }

      (1 to 9).foreach { i =>
        persister ! Persist(s"e-$i")
      }
      persister ! PersistWithAck("e-10", probe.ref)
      probe.expectMessage(Done)

      // Initial pubsub messages are dropped: no backtracking events yet
      result.expectNoMessage(500.millis)

      (1 to 10).foreach { i =>
        val env = result.expectNext()
        env.source shouldBe EnvelopeOrigin.SourceQuery
        env.event shouldBe s"e-$i"
      }

      result.expectNoMessage(1.second)

      // Then the backtracking
      (1 to 10).foreach { i =>
        val env = result.expectNext()
        env.source shouldBe EnvelopeOrigin.SourceBacktracking
        env.sequenceNr shouldBe i
      }

      // Then the pubsub
      (11 to 19).foreach { i =>
        persister ! Persist(s"e-$i")
      }
      persister ! PersistWithAck(s"e-20", probe.ref)

      (11 to 20).foreach { i =>
        val env = result.expectNext()
        env.source shouldBe EnvelopeOrigin.SourcePubSub
        env.event shouldBe s"e-$i"
      }

      // Then regular
      (11 to 20).foreach { i =>
        val env = result.expectNext()
        env.source shouldBe EnvelopeOrigin.SourceQuery
        env.event shouldBe s"e-$i"
      }

      // Then backtracking
      (11 to 20).foreach { i =>
        val env = result.expectNext()
        env.source shouldBe EnvelopeOrigin.SourceBacktracking
        env.sequenceNr shouldBe i
      }

      // Idle
      Thread.sleep(6000)

      // ...but heartbeat will open up the pubsub window again
      persister ! PersistWithAck("e-21", probe.ref)

      {
        val env = result.expectNext()
        env.source shouldBe EnvelopeOrigin.SourcePubSub
        env.event shouldBe "e-21"
      }
    }
  }
}
