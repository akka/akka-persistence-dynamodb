/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.query

import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object EventsBySliceBacktrackingSpec {
  private val BufferSize = 10 // small buffer for testing

  private val config = ConfigFactory
    .parseString(s"""
    akka.persistence.dynamodb.journal.publish-events = off
    akka.persistence.dynamodb.query {
      refresh-interval = 1s
      buffer-size = $BufferSize
    }
    """)
    .withFallback(TestConfig.config)
}

class EventsBySliceBacktrackingSpec
    extends ScalaTestWithActorTestKit(EventsBySliceBacktrackingSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system)
    .readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)

  "eventsBySlices backtracking" should {

    "find old events with earlier timestamp" in {
      // this scenario is handled by the backtracking query
      val entityType = nextEntityType()
      val pid1 = nextPersistenceId(entityType)
      val slice = query.sliceForPersistenceId(pid1.id)
      val pid2 = randomPersistenceIdForSlice(entityType, slice)
      val sinkProbe = TestSink[EventEnvelope[String]]()(system.classicSystem)

      // don't let behind-current-time be a reason for not finding events
      val startTime = InstantFactory.now().minusSeconds(90)

      writeEvent(slice, pid1, 1L, startTime, "e1-1")
      writeEvent(slice, pid1, 2L, startTime.plusMillis(1), "e1-2")

      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, slice, slice, NoOffset)
          .runWith(sinkProbe)
          .request(100)

      val env1 = result.expectNext()
      env1.persistenceId shouldBe pid1.id
      env1.sequenceNr shouldBe 1L
      env1.eventOption shouldBe Some("e1-1")
      env1.source shouldBe EnvelopeOrigin.SourceQuery

      val env2 = result.expectNext()
      env2.persistenceId shouldBe pid1.id
      env2.sequenceNr shouldBe 2L
      env2.eventOption shouldBe Some("e1-2")
      env2.source shouldBe EnvelopeOrigin.SourceQuery

      // first backtracking query kicks in immediately after the first normal query has finished
      // and it also emits duplicates (by design)
      val env3 = result.expectNext()
      env3.persistenceId shouldBe pid1.id
      env3.sequenceNr shouldBe 1L
      env3.source shouldBe EnvelopeOrigin.SourceBacktracking
      // event payload isn't included in backtracking results
      env3.eventOption shouldBe None
      // but it can be lazy loaded
      query.loadEnvelope[String](env3.persistenceId, env3.sequenceNr).futureValue.eventOption shouldBe Some("e1-1")
      // backtracking up to (and equal to) the same offset
      val env4 = result.expectNext()
      env4.persistenceId shouldBe pid1.id
      env4.sequenceNr shouldBe 2L
      env4.eventOption shouldBe None

      result.expectNoMessage(100.millis) // not e1-2

      writeEvent(slice, pid1, 3L, startTime.plusMillis(3), "e1-3")
      val env5 = result.expectNext()
      env5.persistenceId shouldBe pid1.id
      env5.sequenceNr shouldBe 3L

      // before e1-3 so it will not be found by the normal query
      writeEvent(slice, pid2, 1L, startTime.plusMillis(2), "e2-1")

      // no backtracking yet
      result.expectNoMessage(settings.querySettings.refreshInterval + 100.millis)

      // after 1/2 of the backtracking window, to kick off a backtracking query
      writeEvent(
        slice,
        pid1,
        4L,
        startTime.plusMillis(settings.querySettings.backtrackingWindow.toMillis / 2).plusMillis(4),
        "e1-4")
      val env6 = result.expectNext()
      env6.persistenceId shouldBe pid1.id
      env6.sequenceNr shouldBe 4L

      // backtracking finds it,  and it also emits duplicates (by design)
      // e1-1 and e1-2 were already handled by previous backtracking query
      val env7 = result.expectNext()
      env7.persistenceId shouldBe pid2.id
      env7.sequenceNr shouldBe 1L

      val env8 = result.expectNext()
      env8.persistenceId shouldBe pid1.id
      env8.sequenceNr shouldBe 3L

      val env9 = result.expectNext()
      env9.persistenceId shouldBe pid1.id
      env9.sequenceNr shouldBe 4L

      result.cancel()
    }

    "emit from backtracking after first normal query" in {
      val entityType = nextEntityType()
      val pid1 = nextPersistenceId(entityType)
      val slice = query.sliceForPersistenceId(pid1.id)
      val pid2 = randomPersistenceIdForSlice(entityType, slice)
      val sinkProbe = TestSink[EventEnvelope[String]]()

      // don't let behind-current-time be a reason for not finding events
      val startTime = InstantFactory.now().minusSeconds(90)

      writeEvent(slice, pid1, 1L, startTime, "e1-1")
      writeEvent(slice, pid1, 2L, startTime.plusMillis(2), "e1-2")
      writeEvent(slice, pid1, 3L, startTime.plusMillis(4), "e1-3")

      def startQuery(offset: Offset): TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, slice, slice, offset)
          .runWith(sinkProbe)
          .request(100)

      def expect(env: EventEnvelope[String], pid: PersistenceId, seqNr: Long, eventOption: Option[String]): Offset = {
        env.persistenceId shouldBe pid.id
        env.sequenceNr shouldBe seqNr
        env.eventOption shouldBe eventOption
        env.offset
      }

      val result1 = startQuery(NoOffset)
      expect(result1.expectNext(), pid1, 1L, Some("e1-1"))
      expect(result1.expectNext(), pid1, 2L, Some("e1-2"))
      expect(result1.expectNext(), pid1, 3L, Some("e1-3"))

      // first backtracking query kicks in immediately after the first normal query has finished
      // and it also emits duplicates (by design)
      expect(result1.expectNext(), pid1, 1L, None)
      expect(result1.expectNext(), pid1, 2L, None)
      val offset1 = expect(result1.expectNext(), pid1, 3L, None)
      result1.cancel()

      // write delayed events from pid2
      writeEvent(slice, pid2, 1L, startTime.plusMillis(1), "e2-1")
      writeEvent(slice, pid2, 2L, startTime.plusMillis(3), "e2-2")
      writeEvent(slice, pid2, 3L, startTime.plusMillis(5), "e2-3")

      val result2 = startQuery(offset1)
      // backtracking
      expect(result2.expectNext(), pid1, 1L, None)
      expect(result2.expectNext(), pid2, 1L, None)
      expect(result2.expectNext(), pid1, 2L, None)
      expect(result2.expectNext(), pid2, 2L, None)
      expect(result2.expectNext(), pid1, 3L, None)
      // from normal query
      expect(result2.expectNext(), pid2, 3L, Some("e2-3"))

      result2.cancel()
    }

    "not backtrack when too far behind current time" in {
      val entityType = nextEntityType()
      val pid1 = nextPersistenceId(entityType)
      val slice = query.sliceForPersistenceId(pid1.id)
      val pid2 = randomPersistenceIdForSlice(entityType, slice)
      val sinkProbe = TestSink[EventEnvelope[String]]()

      // one day behind
      val startTime = InstantFactory.now().minusSeconds(60 * 60 * 24)

      (1 to 100).foreach { n =>
        writeEvent(slice, pid1, n, startTime.plusSeconds(n).plusMillis(1), s"e1-$n")
        writeEvent(slice, pid2, n, startTime.plusSeconds(n).plusMillis(2), s"e2-$n")
      }

      def startQuery(offset: Offset): TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, slice, slice, offset)
          .runWith(sinkProbe)
          .request(204)

      def expect(env: EventEnvelope[String], pid: PersistenceId, seqNr: Long, eventOption: Option[String]): Offset = {
        env.persistenceId shouldBe pid.id
        env.sequenceNr shouldBe seqNr

        if (eventOption.isEmpty) env.source shouldBe EnvelopeOrigin.SourceBacktracking
        else env.source shouldBe EnvelopeOrigin.SourceQuery

        env.eventOption shouldBe eventOption

        env.offset
      }

      val result1 = startQuery(NoOffset)
      (1 to 100).foreach { n =>
        // These are Some events, viz. they come from the query, not from backtracking
        expect(result1.expectNext(), pid1, n, Some(s"e1-$n"))
        expect(result1.expectNext(), pid2, n, Some(s"e2-$n"))
      }

      result1.expectNoMessage()

      val recent = InstantFactory.now().minus(settings.querySettings.backtrackingBehindCurrentTime.toJava)
      writeEvent(slice, pid1, 101, recent, "e1-101")
      writeEvent(slice, pid2, 101, recent.plusMillis(1), "e2-101")

      // live
      expect(result1.expectNext(), pid1, 101, Some("e1-101"))
      expect(result1.expectNext(), pid2, 101, Some("e2-101"))

      // backtracking
      expect(result1.expectNext(), pid1, 101, None)
      expect(result1.expectNext(), pid2, 101, None)

      result1.cancel()
    }

    "still make initial backtracking until caught up to start offset, then skip backtracking" in {
      val entityType = nextEntityType()
      val pid1 = nextPersistenceId(entityType)
      val slice = query.sliceForPersistenceId(pid1.id)
      val pid2 = randomPersistenceIdForSlice(entityType, slice)
      val sinkProbe = TestSink[EventEnvelope[String]]()

      // one day behind
      val startTime = InstantFactory.now().minusSeconds(60 * 60 * 24)

      (1 to 4).foreach { n =>
        // odd: pid1, even: pid2
        val (pid, mod) = if (n % 2 == 1) (pid1, 1) else (pid2, 2)
        val seqNr = (n + 1) / 2
        writeEvent(slice, pid, seqNr, startTime.plusMillis(n), s"e${mod}-${seqNr}")
      }

      // will start query at next event
      val startOffset = TimestampOffset(startTime.plusSeconds(23).plusMillis(1), Map.empty)

      // go past switch-to-backtracking trigger of 3 * buffer size (of 10)
      (3 to 30).foreach { n =>
        writeEvent(slice, pid1, n, startTime.plusSeconds(20 + n).plusMillis(1), s"e1-$n")
        writeEvent(slice, pid2, n, startTime.plusSeconds(20 + n).plusMillis(2), s"e2-$n")
      }

      def startQuery(offset: Offset): TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, slice, slice, offset)
          .runWith(sinkProbe)
          .request(1000)

      def expect(env: EventEnvelope[String], pid: PersistenceId, seqNr: Long, eventOption: Option[String]): Offset = {
        env.persistenceId shouldBe pid.id
        env.sequenceNr shouldBe seqNr

        if (eventOption.isEmpty) env.source shouldBe EnvelopeOrigin.SourceBacktracking
        else env.source shouldBe EnvelopeOrigin.SourceQuery

        env.eventOption shouldBe eventOption

        env.offset
      }

      val result1 = startQuery(startOffset)
      // from backtracking
      expect(result1.expectNext(), pid1, 1, None)
      expect(result1.expectNext(), pid2, 1, None)
      expect(result1.expectNext(), pid1, 2, None)
      expect(result1.expectNext(), pid2, 2, None)
      expect(result1.expectNext(), pid1, 3, None) // start offset

      // from normal
      (3 to 30).foreach { n =>
        expect(result1.expectNext(), pid1, n, Some(s"e1-$n"))
        expect(result1.expectNext(), pid2, n, Some(s"e2-$n"))
      }

      result1.expectNoMessage()

      result1.cancel()
    }
  }

}
