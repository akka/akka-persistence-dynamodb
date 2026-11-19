/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal.query.internal

import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic

/** INTERNAL API */
@InternalApi
private[query] final class MergeHotWithCoolerStage[Event](
    hotSource: Source[EventEnvelope[Event], NotUsed],
    coolerSource: Option[TimestampOffset] => Source[EventEnvelope[Event], NotUsed],
    initialCoolerOffset: Option[TimestampOffset],
    hotAheadOfWatermark: FiniteDuration,
    consistencyDelay: FiniteDuration)
    extends GraphStage[SourceShape[EventEnvelope[Event]]] {
  require(consistencyDelay.toMillis > 0, "consistencyDelay must be at least one millisecond")
  require(hotAheadOfWatermark >= consistencyDelay)

  private val proactiveRefreshThresholdMillis =
    (hotAheadOfWatermark.toMillis + consistencyDelay.toMillis) / 2
  private val refreshAfterProgressMillis = consistencyDelay.toMillis / 2

  val out = Outlet[EventEnvelope[Event]](s"MergeHotWithCooler.out")
  override val shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with OutHandler {
      private case object RefreshCooler

      private var hotIn: SubSinkInlet[EventEnvelope[Event]] = _
      private var coolerIn: SubSinkInlet[EventEnvelope[Event]] = _

      private var hotPending: Option[EventEnvelope[Event]] = None
      private var coolerPending: Option[EventEnvelope[Event]] = None
      private var hotFinished = false
      private var coolerActive = false
      private var coolerOffset = initialCoolerOffset
      private var coolerWatermark = Instant.MIN
      private var coolerQueryStarted = Instant.MIN
      private var coolerQueryAdvanced = false
      private var refreshAfterProgressAt: Option[Instant] = None
      private var refreshScheduledFor: Option[Instant] = None
      private var emittedOrPendingHighWatermark = Instant.MIN

      setHandler(out, this)

      override def preStart(): Unit = {
        materializeHot()
        materializeCooler()
      }

      override def onPull(): Unit = pushIfAble()

      override def onDownstreamFinish(cause: Throwable): Unit = cancelStage(cause)

      override protected def onTimer(timerKey: Any): Unit =
        timerKey match {
          case RefreshCooler =>
            refreshScheduledFor = None
            ensureCoolerProgress()
          case _ =>
        }

      private def materializeHot(): Unit = {
        hotIn = new SubSinkInlet[EventEnvelope[Event]]("MergeHotWithCooler.hot")
        hotIn.setHandler(new InHandler {
          override def onPush(): Unit = {
            hotPending = Some(hotIn.grab())
            advanceHighWatermark(eventTimestamp(hotPending.get))
            pushIfAble()
            ensureCoolerProgress()
          }

          override def onUpstreamFinish(): Unit = {
            hotFinished = true
            pushIfAble()
            completeIfDone()
          }
        })
        hotSource.runWith(hotIn.sink)(subFusingMaterializer)
        hotIn.pull()
      }

      private def materializeCooler(): Unit = {
        cancelTimer(RefreshCooler)
        refreshScheduledFor = None
        refreshAfterProgressAt = None
        coolerActive = true
        coolerQueryAdvanced = false
        coolerQueryStarted = Instant.now()
        coolerIn = new SubSinkInlet[EventEnvelope[Event]]("MergeHotWithCooler.cooler")
        coolerIn.setHandler(new InHandler {
          override def onPush(): Unit = {
            coolerPending = Some(coolerIn.grab())
            advanceHighWatermark(eventTimestamp(coolerPending.get))
            pushIfAble()
          }

          override def onUpstreamFinish(): Unit = {
            coolerActive = false
            if (coolerQueryAdvanced)
              refreshAfterProgressAt = Some(Instant.now().plusMillis(refreshAfterProgressMillis))
            val completedThrough = coolerQueryStarted.minusMillis(consistencyDelay.toMillis)
            advanceWatermark(completedThrough)
            pushIfAble()
            completeIfDone()
            ensureCoolerProgress()
          }
        })
        val startFrom = coolerOffset.map { offset =>
          if (coolerWatermark.isAfter(offset.timestamp)) TimestampOffset(coolerWatermark, Map.empty)
          else offset
        }
        coolerSource(backtrack(startFrom)).runWith(coolerIn.sink)(subFusingMaterializer)
        coolerIn.pull()
      }

      private def pushIfAble(): Unit =
        if (isAvailable(out)) {
          (coolerPending, hotPending) match {
            case (Some(cooler), Some(hot)) =>
              if (!eventTimestamp(cooler).isAfter(eventTimestamp(hot))) pushCooler(cooler)
              else pushHot(hot)

            case (Some(cooler), None) =>
              pushCooler(cooler)

            case (None, Some(hot)) if hotCanBeEmitted(hot) =>
              pushHot(hot)

            case _ =>
              completeIfDone()
              ensureCoolerProgress()
          }
        }

      private def pushHot(event: EventEnvelope[Event]): Unit = {
        hotPending = None
        push(out, event)
        pullIfOpen(hotIn)
        completeIfDone()
      }

      private def pushCooler(event: EventEnvelope[Event]): Unit = {
        val offset = timestampOffset(event)
        advanceCoolerOffset(offset)
        advanceWatermark(offset.timestamp)
        coolerPending = None
        push(out, event)
        pullIfOpen(coolerIn)
        completeIfDone()
      }

      private def pullIfOpen(in: SubSinkInlet[EventEnvelope[Event]]): Unit =
        if (!in.isClosed && !in.hasBeenPulled) in.pull()

      private def hotCanBeEmitted(event: EventEnvelope[Event]): Boolean =
        !eventTimestamp(event).isAfter(coolerWatermark.plusMillis(hotAheadOfWatermark.toMillis))

      private def eventTimestamp(event: EventEnvelope[Event]): Instant = timestampOffset(event).timestamp

      private def timestampOffset(event: EventEnvelope[Event]): TimestampOffset =
        event.offset match {
          case offset: TimestampOffset => offset
          case other =>
            throw new IllegalArgumentException(
              s"Expected TimestampOffset for [${event.persistenceId}, ${event.sequenceNr}], got [$other]")
        }

      private def advanceWatermark(candidate: Instant): Unit =
        if (candidate.isAfter(coolerWatermark)) coolerWatermark = candidate

      private def advanceHighWatermark(candidate: Instant): Unit =
        if (candidate.isAfter(emittedOrPendingHighWatermark)) emittedOrPendingHighWatermark = candidate

      private def advanceCoolerOffset(candidate: TimestampOffset): Unit =
        coolerOffset match {
          case None =>
            coolerOffset = Some(candidate)
            coolerQueryAdvanced = true

          case Some(current) if candidate.timestamp.isAfter(current.timestamp) =>
            coolerOffset = Some(candidate)
            coolerQueryAdvanced = true

          case Some(current) if candidate.timestamp == current.timestamp =>
            val mergedSeen = candidate.seen.foldLeft(current.seen) { case (seen, (persistenceId, sequenceNr)) =>
              if (seen.get(persistenceId).forall(_ < sequenceNr)) seen.updated(persistenceId, sequenceNr)
              else seen
            }
            if (mergedSeen != current.seen) {
              coolerOffset = Some(TimestampOffset(current.timestamp, candidate.readTimestamp, mergedSeen))
              coolerQueryAdvanced = true
            }

          case _ => // duplicate from the backtracking window
        }

      private def backtrack(offset: Option[TimestampOffset]): Option[TimestampOffset] =
        offset.map { current =>
          val backtrackedTimestamp =
            if (current.timestamp.isBefore(Instant.EPOCH.plusMillis(consistencyDelay.toMillis))) Instant.EPOCH
            else current.timestamp.minusMillis(consistencyDelay.toMillis)
          TimestampOffset(backtrackedTimestamp, Map.empty)
        }

      private def ensureCoolerProgress(): Unit =
        if (!coolerActive && coolerPending.isEmpty && !(hotFinished && hotPending.isEmpty)) {
          val eventDrivenRefreshAt =
            if (emittedOrPendingHighWatermark
                .isAfter(coolerWatermark.plusMillis(proactiveRefreshThresholdMillis)))
              Some(
                emittedOrPendingHighWatermark
                  .minusMillis(proactiveRefreshThresholdMillis)
                  .plusMillis(consistencyDelay.toMillis))
            else None

          val wallClockRefreshAt = coolerWatermark
            .plusMillis(proactiveRefreshThresholdMillis)
            .plusMillis(consistencyDelay.toMillis)

          val refreshCandidates = refreshAfterProgressAt.toSeq ++ eventDrivenRefreshAt.toSeq :+ wallClockRefreshAt
          refreshCandidates.minOption.foreach { refreshAt =>
            val now = Instant.now()
            val delay = JDuration.between(now, refreshAt)
            if (delay.isNegative || delay.isZero) materializeCooler()
            else if (refreshScheduledFor.forall(refreshAt.isBefore)) {
              cancelTimer(RefreshCooler)
              refreshScheduledFor = Some(refreshAt)
              scheduleOnce(RefreshCooler, delay.toScala)
            }
          }
        }

      private def completeIfDone(): Unit =
        if (hotFinished && hotPending.isEmpty && !coolerActive && coolerPending.isEmpty) completeStage()
    }
}
