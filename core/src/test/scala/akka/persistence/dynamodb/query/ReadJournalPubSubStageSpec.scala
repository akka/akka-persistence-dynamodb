/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.query

import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.internal.pubsub.TopicImpl
import akka.actor.typed.pubsub.Topic
import akka.persistence.SerializedEvent
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestInstrumentation
import akka.persistence.dynamodb.TestInstrumentationProtocol.PubSubEventDropped
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.wordspec.AnyWordSpecLike
import akka.persistence.query.Offset
import akka.serialization.Serializers

class ReadJournalPubSubStageSpec extends ScalaTestWithActorTestKit(TestConfig.config) with AnyWordSpecLike {
  def _topicProbe(name: String): TestProbe[Topic.Command[EventEnvelope[String]]] = TestProbe(name)
  def createInstrumentation() = new TestInstrumentation(system)

  def expectTopicSubscribe(
      probe: TestProbe[Topic.Command[EventEnvelope[String]]]): TopicImpl.Subscribe[EventEnvelope[String]] =
    probe.expectMessageType[TopicImpl.Subscribe[EventEnvelope[String]]]

  var globalSequenceNumberTracker = 0L
  def event(pid: String, evt: String, seqNr: Long, timestamp: Long, slice: Int): EventEnvelope[String] = {
    globalSequenceNumberTracker += 1
    val offset = Offset.sequence(globalSequenceNumberTracker)
    EventEnvelope(offset, pid, seqNr, evt, timestamp, "SomeEntity", slice)
  }

  def expectDropped(instrumentation: TestInstrumentation) =
    instrumentation.probe.expectMessageType[PubSubEventDropped]

  "A PubSubSourceStage" should {
    "subscribe to the topics and watch" in {
      val topicProbes = Seq(_topicProbe("slice-0-255"), _topicProbe("slice-256-511"))
      val instrumentation = createInstrumentation()
      val stage = new DynamoDBReadJournal.PubSubSourceStage[String](
        entityType = "SomeEntity",
        minSlice = 0,
        maxSlice = 511,
        bufferLimit = 10,
        deserializeEvent = _ => fail("should not be called"),
        eventTopics = topicProbes.map(_.ref).toSet,
        instrumentation)

      val sinkProbe = Source.fromGraph(stage).runWith(TestSink())
      topicProbes.iterator.map(expectTopicSubscribe).toSet.size shouldBe 1
      sinkProbe.request(1)
      sinkProbe.expectNoMessage(30.millis) // should not create events when none come through subscription
      topicProbes.head.stop()
      sinkProbe.expectError().getMessage shouldBe "Pubsub topic actor terminated"
    }

    "only emit enevelopes for slices in range and drop envelopes (with no instrumentation) for slices out of range" in {
      val now = System.currentTimeMillis()
      val topicProbe = _topicProbe("slice-0-1023")
      val instrumentation = createInstrumentation()
      val stage = new DynamoDBReadJournal.PubSubSourceStage[String](
        entityType = "SomeEntity",
        minSlice = 0,
        maxSlice = 511,
        bufferLimit = 10,
        deserializeEvent = _ => fail("should not be called"),
        eventTopics = Set(topicProbe.ref),
        instrumentation)

      val sinkProbe = Source.fromGraph(stage).runWith(TestSink())
      val sourceActor = expectTopicSubscribe(topicProbe).subscriber
      sinkProbe.request(1)
      // Probably lying about the slice, but the subscriber assumes the topics are correct
      sourceActor ! event("P1", "one", 42, now, 123)
      sourceActor ! event("P2", "two", 21, now, 567)
      sourceActor ! event("P3", "tre", 14, now + 1, 234)
      val firstEmitted = sinkProbe.requestNext()
      // This is an expected drop, no instrumentation needed
      instrumentation.probe.expectNoMessage(30.millis)

      firstEmitted.persistenceId shouldBe "P1"
      firstEmitted.event shouldBe "one"
      firstEmitted.sequenceNr shouldBe 42
      firstEmitted.slice shouldBe 123
      val secondEmitted = sinkProbe.requestNext()
      secondEmitted.persistenceId shouldBe "P3"
      sinkProbe.expectNoMessage(10.millis)
    }

    "accept up to the buffer for slices in range and then drop (with instrumentation) later envelopes" in {
      val now = System.currentTimeMillis()
      val topicProbe = _topicProbe("slice-0-1023")
      val instrumentation = createInstrumentation()
      val stage = new DynamoDBReadJournal.PubSubSourceStage[String](
        entityType = "SomeEntity",
        minSlice = 0,
        maxSlice = 511,
        bufferLimit = 5,
        deserializeEvent = _ => fail("should not be called"),
        eventTopics = Set(topicProbe.ref),
        instrumentation)

      val sinkProbe = Source.fromGraph(stage).runWith(TestSink())
      val sourceActor = expectTopicSubscribe(topicProbe).subscriber
      // no demand yet, fill that buffer
      sourceActor ! event("P1", "four", 1, now, 123)
      sourceActor ! event("P2", "five", 777, now, 567)
      sourceActor ! event("P1", "six", 2, now + 5, 123)
      sourceActor ! event("P1", "eight", 3, now + 11, 123)
      sourceActor ! event("P3", "nine", 350, now + 8, 234)
      sourceActor ! event("P4", "ten", 919, now, 345)
      // buffer now full
      sourceActor ! event("P5", "onze", 52, now + 20, 678)
      sourceActor ! event("P1", "douze", 4, now + 100, 123)
      val dropped = expectDropped(instrumentation)
      val emitted = (1 to 5).iterator.map(_ => sinkProbe.requestNext()).toSeq
      sinkProbe.expectNoMessage(30.millis)
      instrumentation.probe.expectNoMessage(10.millis)

      dropped.persistenceId shouldBe "P1"
      dropped.entityType shouldBe "SomeEntity"
      dropped.sequenceNumber shouldBe 4

      (emitted.map { env =>
        env.persistenceId -> env.sequenceNr
      } should contain).theSameElementsInOrderAs(Seq("P1" -> 1, "P1" -> 2, "P1" -> 3, "P3" -> 350, "P4" -> 919))
    }

    "deserialize pre-serialized events" in {
      val now = System.currentTimeMillis()
      val topicProbe = _topicProbe("slice-0-1023")
      val instrumentation = createInstrumentation()
      val serialization = SerializationExtension(system)
      val serializer = serialization.serializerFor(classOf[String])
      val stage = new DynamoDBReadJournal.PubSubSourceStage[String](
        entityType = "SomeEntity",
        minSlice = 0,
        maxSlice = 511,
        bufferLimit = 5,
        deserializeEvent = { se =>
          val bytes = se.bytes
          val id = se.serializerId
          val manifest = se.serializerManifest
          serialization.deserialize(bytes, id, manifest).get.asInstanceOf[String]
        },
        eventTopics = Set(topicProbe.ref),
        instrumentation)

      val sinkProbe = Source.fromGraph(stage).runWith(TestSink())
      val sourceActor = expectTopicSubscribe(topicProbe).subscriber
      val serId = serializer.identifier
      val manifest = Serializers.manifestFor(serializer, "hello")
      sourceActor ! event("P7", "", 1, now, 123).withEventOption(
        Some(new SerializedEvent(serializer.toBinary("hello"), serId, manifest)).asInstanceOf[Option[String]])

      sinkProbe.requestNext().event shouldBe "hello"
    }
  }
}
