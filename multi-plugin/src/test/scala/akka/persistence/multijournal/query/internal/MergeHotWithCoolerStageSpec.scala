/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal.query.internal

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.DurationInt

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.NotUsed
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike

class MergeHotWithCoolerStageSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "MergeHotWithCoolerStage" should {
    "merge a finite cooler query with the hot source in timestamp order" in {
      val base = Instant.now().minusSeconds(1)
      val cooler = Seq(envelope(1, base.plusMillis(10)), envelope(3, base.plusMillis(30)))
      val hot = Seq(envelope(2, base.plusMillis(20)), envelope(4, base.plusMillis(40)))
      val coolerQueries = new AtomicInteger

      val result = Source
        .fromGraph(
          new MergeHotWithCoolerStage(
            Source(hot),
            _ => {
              coolerQueries.incrementAndGet()
              Source(cooler)
            },
            None,
            1.second,
            100.millis))
        .runWith(Sink.seq)
        .futureValue

      result.map(_.sequenceNr) shouldBe Seq(1L, 2L, 3L, 4L)
      coolerQueries.get shouldBe 1
    }

    "repeat the cooler query when the hot source moves beyond the watermark" in {
      val base = Instant.now()
      val coolerEvent = envelope(1, base.plusMillis(50))
      val hotEvent = envelope(2, base.plusMillis(300))
      val coolerQueries = new AtomicInteger
      val repeatedFrom = new AtomicReference[Option[TimestampOffset]](None)

      val result = Source
        .fromGraph(
          new MergeHotWithCoolerStage(
            Source.single(hotEvent),
            offset =>
              if (coolerQueries.incrementAndGet() == 1) Source.single(coolerEvent)
              else {
                repeatedFrom.compareAndSet(None, offset)
                Source.empty
              },
            None,
            100.millis,
            20.millis))
        .take(2)
        .runWith(Sink.seq)
        .futureValue

      result.map(_.sequenceNr) shouldBe Seq(1L, 2L)
      coolerQueries.get should be >= 2
      repeatedFrom.get().map(_.timestamp) shouldBe Some(base.plusMillis(30))
      repeatedFrom.get().map(_.seen) shouldBe Some(Map.empty)
    }

    "backtrack the initial cooler offset by the consistency delay" in {
      val base = Instant.now()
      val initialOffset = TimestampOffset(base, base, Map("entity|0" -> 10L))
      val queriedFrom = new AtomicReference[Option[TimestampOffset]](None)

      Source
        .fromGraph(
          new MergeHotWithCoolerStage(
            Source.empty,
            offset => {
              queriedFrom.set(offset)
              Source.empty
            },
            Some(initialOffset),
            1.second,
            100.millis))
        .runWith(Sink.ignore)
        .futureValue

      queriedFrom.get().map(_.timestamp) shouldBe Some(base.minusMillis(100))
      queriedFrom.get().map(_.seen) shouldBe Some(Map.empty)
    }

    "refresh after a cooler query advances its offset" in {
      val base = Instant.now().minusSeconds(1)
      val coolerQueries = new AtomicInteger

      val (killSwitch, _) = Source
        .fromGraph(
          new MergeHotWithCoolerStage(
            Source.maybe[EventEnvelope[String]].mapMaterializedValue(_ => NotUsed),
            _ =>
              if (coolerQueries.incrementAndGet() == 1) Source.single(envelope(1, base))
              else Source.empty,
            None,
            1.second,
            200.millis))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

      eventually {
        coolerQueries.get should be >= 2
      }
      killSwitch.shutdown()
    }

    "refresh proactively before the hot event reaches the emission limit" in {
      val coolerQueries = new AtomicInteger

      val (killSwitch, _) = Source
        .fromGraph(
          new MergeHotWithCoolerStage(
            // timestamp relative to when the stage starts, so slow materialization doesn't eat the margin
            Source
              .lazySingle(() => envelope(1, Instant.now().plusMillis(350)))
              .concat(Source.maybe)
              .mapMaterializedValue(_ => NotUsed),
            _ => {
              coolerQueries.incrementAndGet()
              Source.empty
            },
            None,
            500.millis,
            100.millis))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

      eventually {
        coolerQueries.get should be >= 2
      }
      killSwitch.shutdown()
    }

    "not repeatedly re-emit old cooler events while the hot source makes progress" in {
      val startedAt = Instant.now()
      val oldCoolerEvent = envelope(1, startedAt.minusSeconds(3600))
      val coolerQueries = new AtomicInteger
      val lastQueriedFrom = new AtomicReference[Option[TimestampOffset]](None)
      val hotSeqNr = new AtomicInteger(100)

      // steady hot traffic, with timestamps at the current time
      val hot = Source
        .tick(0.millis, 20.millis, ())
        .map(_ => envelope(hotSeqNr.incrementAndGet().toLong, Instant.now()))
        .mapMaterializedValue(_ => NotUsed)

      // behaves like currentEventsBySlices: events at or after the offset timestamp which aren't in seen
      val cooler = (offset: Option[TimestampOffset]) => {
        coolerQueries.incrementAndGet()
        lastQueriedFrom.set(offset)
        val visible = offset.forall { o =>
          !oldCoolerEvent.offset.asInstanceOf[TimestampOffset].timestamp.isBefore(o.timestamp) &&
          !o.seen.get(oldCoolerEvent.persistenceId).exists(_ >= oldCoolerEvent.sequenceNr)
        }
        if (visible) Source.single(oldCoolerEvent) else Source.empty
      }

      val result = Source
        .fromGraph(new MergeHotWithCoolerStage(hot, cooler, None, 200.millis, 50.millis))
        .takeWithin(2.seconds)
        .runWith(Sink.seq)
        .futureValue

      // the cooler journals keep being re-queried...
      coolerQueries.get should be >= 5
      // ...but once cooler progress has moved past the old event, it is no longer re-read
      result.count(_.sequenceNr == oldCoolerEvent.sequenceNr) should (be >= 1 and be <= 3)
      lastQueriedFrom.get().map(_.timestamp).exists(_.isAfter(startedAt)) shouldBe true
      // hot events are still emitted in timestamp order
      val hotTimestamps = result.filter(_.sequenceNr > 100).map(_.offset.asInstanceOf[TimestampOffset].timestamp)
      hotTimestamps should not be empty
      hotTimestamps shouldBe hotTimestamps.sorted
    }
  }

  private def envelope(sequenceNr: Long, timestamp: Instant): EventEnvelope[String] = {
    val persistenceId = s"entity|$sequenceNr"
    new EventEnvelope(
      TimestampOffset(timestamp, timestamp, Map(persistenceId -> sequenceNr)),
      persistenceId,
      sequenceNr,
      Some(s"event-$sequenceNr"),
      timestamp.toEpochMilli,
      None,
      "entity",
      0,
      filtered = false,
      source = "test",
      tags = Set.empty)
  }
}
