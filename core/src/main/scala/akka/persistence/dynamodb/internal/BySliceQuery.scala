/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Clock
import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import org.slf4j.Logger

/**
 * INTERNAL API
 */
@InternalApi private[dynamodb] object BySliceQuery {

  object QueryState {
    val empty: QueryState =
      QueryState(
        latest = TimestampOffset.Zero,
        itemCount = 0,
        itemCountSinceBacktracking = 0,
        queryCount = 0,
        idleCount = 0,
        backtrackingCount = 0,
        latestBacktracking = TimestampOffset.Zero,
        latestBacktrackingSeenCount = 0,
        backtrackingExpectFiltered = 0,
        previous = TimestampOffset.Zero,
        previousBacktracking = TimestampOffset.Zero,
        startTimestamp = Instant.EPOCH,
        startWallClock = Instant.EPOCH,
        currentQueryWallClock = Instant.EPOCH,
        previousQueryWallClock = Instant.EPOCH,
        idleCountBeforeHeartbeat = 0)
  }

  final case class QueryState(
      latest: TimestampOffset,
      itemCount: Int,
      itemCountSinceBacktracking: Long,
      queryCount: Long,
      idleCount: Long,
      backtrackingCount: Int,
      latestBacktracking: TimestampOffset,
      latestBacktrackingSeenCount: Int,
      backtrackingExpectFiltered: Int,
      previous: TimestampOffset,
      previousBacktracking: TimestampOffset,
      startTimestamp: Instant,
      startWallClock: Instant,
      currentQueryWallClock: Instant,
      previousQueryWallClock: Instant,
      idleCountBeforeHeartbeat: Long) {

    def backtracking: Boolean = backtrackingCount > 0

    def currentOffset: TimestampOffset =
      if (backtracking) latestBacktracking
      else latest

    def nextQueryFromTimestamp(backtrackingWindow: JDuration): Instant =
      if (backtracking) {
        if (latest.timestamp.minus(backtrackingWindow).isAfter(latestBacktracking.timestamp))
          latest.timestamp.minus(backtrackingWindow)
        else latestBacktracking.timestamp
      } else latest.timestamp

    def nextQueryToTimestamp: Option[Instant] = {
      if (backtracking) Some(latest.timestamp)
      else None
    }
  }

  trait SerializedItem {
    def persistenceId: String
    def seqNr: Long
    def eventTimestamp: Instant
    def readTimestamp: Instant
    def source: String
  }

  trait Dao[Item] {
    def itemsBySlice(
        entityType: String,
        slice: Int,
        fromTimestamp: Instant,
        toTimestamp: Instant,
        backtracking: Boolean): Source[Item, NotUsed]
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[dynamodb] class BySliceQuery[Item <: BySliceQuery.SerializedItem, Envelope](
    dao: BySliceQuery.Dao[Item],
    createEnvelope: (TimestampOffset, Item) => Envelope,
    extractOffset: Envelope => TimestampOffset,
    createHeartbeat: Instant => Option[Envelope],
    clock: Clock,
    settings: DynamoDBSettings,
    log: Logger)(implicit val ec: ExecutionContext) {
  import BySliceQuery._
  import TimestampOffset.toTimestampOffset

  private val backtrackingWindow = JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)
  private val halfBacktrackingWindow = backtrackingWindow.dividedBy(2)
  private val backtrackingBehindCurrentTime =
    JDuration.ofMillis(settings.querySettings.backtrackingBehindCurrentTime.toMillis)
  private val firstBacktrackingQueryWindow = backtrackingWindow.plus(backtrackingBehindCurrentTime)

  def currentBySlice(
      logPrefix: String,
      entityType: String,
      slice: Int,
      offset: Offset,
      filterEventsBeforeSnapshots: (String, Long, String) => Boolean = (_, _, _) => true): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    def nextOffset(state: QueryState, envelope: Envelope): QueryState = {
      if (EnvelopeOrigin.isHeartbeatEvent(envelope)) state
      else state.copy(latest = extractOffset(envelope), itemCount = state.itemCount + 1)
    }

    def nextQuery(state: QueryState, endTimestamp: Instant): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      // Note that we can't know how many events with the same timestamp that are filtered out
      // so continue until itemCount is 0. That means an extra query at the end to make sure there are no
      // more to fetch.
      if (state.queryCount == 0L || state.itemCount > 0) {
        val newState = state.copy(itemCount = 0, queryCount = state.queryCount + 1, previous = state.latest)

        val toTimestamp = newState.nextQueryToTimestamp match {
          case Some(t) =>
            if (t.isBefore(endTimestamp)) t else endTimestamp
          case None =>
            endTimestamp
        }

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "{} next query [{}] from slice [{}], between time [{} - {}]. Found [{}] items in previous query.",
            logPrefix,
            state.queryCount,
            slice,
            state.latest.timestamp,
            toTimestamp,
            state.itemCount)

        newState -> Some(
          dao
            .itemsBySlice(entityType, slice, state.latest.timestamp, toTimestamp, backtracking = false)
            .filter { item =>
              filterEventsBeforeSnapshots(item.persistenceId, item.seqNr, item.source)
            }
            .via(deserializeAndAddOffset(state.latest)))
      } else {
        if (log.isDebugEnabled)
          log.debug(
            "{} query [{}] from slice [{}] completed. Found [{}] items in previous query.",
            logPrefix,
            state.queryCount,
            slice,
            state.itemCount)

        state -> None
      }
    }

    val currentTimestamp = InstantFactory.now()
    if (log.isDebugEnabled())
      log.debug(
        "{} query slice [{}], from time [{}] until now [{}].",
        logPrefix,
        slice,
        initialOffset.timestamp,
        currentTimestamp)

    ContinuousQuery[QueryState, Envelope](
      initialState = QueryState.empty.copy(latest = initialOffset),
      updateState = nextOffset,
      delayNextQuery = _ => None,
      nextQuery = state => nextQuery(state, currentTimestamp),
      beforeQuery = _ => None)

  }

  def liveBySlice(
      logPrefix: String,
      entityType: String,
      slice: Int,
      offset: Offset,
      filterEventsBeforeSnapshots: (String, Long, String) => Boolean = (_, _, _) => true): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    if (log.isDebugEnabled())
      log.debug("Starting {} query from slice [{}], from time [{}].", logPrefix, slice, initialOffset.timestamp)

    def nextOffset(state: QueryState, envelope: Envelope): QueryState = {
      if (EnvelopeOrigin.isHeartbeatEvent(envelope)) state
      else {
        val offset = extractOffset(envelope)
        if (state.backtracking) {
          if (offset.timestamp.isBefore(state.latestBacktracking.timestamp))
            throw new IllegalArgumentException(
              s"Unexpected offset [$offset] before latestBacktracking [${state.latestBacktracking}].")

          val newSeenCount =
            if (offset.timestamp == state.latestBacktracking.timestamp) state.latestBacktrackingSeenCount + 1
            else 1

          state.copy(
            latestBacktracking = offset,
            latestBacktrackingSeenCount = newSeenCount,
            itemCount = state.itemCount + 1)

        } else {
          if (offset.timestamp.isBefore(state.latest.timestamp))
            throw new IllegalArgumentException(s"Unexpected offset [$offset] before latest [${state.latest}].")

          if (log.isDebugEnabled()) {
            if (state.latestBacktracking.seen.nonEmpty &&
              offset.timestamp.isAfter(state.latestBacktracking.timestamp.plus(firstBacktrackingQueryWindow)))
              log.debug(
                "{} next offset is outside the backtracking window, latestBacktracking: [{}], offset: [{}]",
                logPrefix,
                state.latestBacktracking,
                offset)
          }

          state.copy(latest = offset, itemCount = state.itemCount + 1)
        }
      }
    }

    def delayNextQuery(state: QueryState): Option[FiniteDuration] = {
      if (switchFromBacktracking(state)) {
        // switch from backtracking immediately
        None
      } else {
        val delay = ContinuousQuery.adjustNextDelay(
          state.itemCount,
          settings.querySettings.bufferSize,
          settings.querySettings.refreshInterval)

        if (log.isDebugEnabled)
          delay.foreach { d =>
            log.debug(
              "{} query [{}] from slice [{}] delay next [{}] ms.",
              logPrefix,
              state.queryCount,
              slice,
              d.toMillis)
          }

        delay
      }
    }

    def switchFromBacktracking(state: QueryState): Boolean = {
      state.backtracking && state.itemCount < settings.querySettings.bufferSize - state.backtrackingExpectFiltered
    }

    def switchToBacktracking(state: QueryState, newIdleCount: Long): Boolean = {
      // Note that when starting the query with offset = NoOffset, it will try to switch to
      // backtracking immediately after the first normal query because
      // between(latestBacktracking.timestamp, latest.timestamp) > halfBacktrackingWindow

      val qSettings = settings.querySettings

      def disableBacktrackingWhenFarBehindCurrentWallClockTime: Boolean = {
        val aheadOfInitial =
          initialOffset == TimestampOffset.Zero || state.latestBacktracking.timestamp.isAfter(initialOffset.timestamp)

        val previousTimestamp =
          if (state.previous == TimestampOffset.Zero) state.latest.timestamp
          else state.previous.timestamp

        aheadOfInitial && previousTimestamp.isBefore(clock.instant().minus(firstBacktrackingQueryWindow))
      }

      qSettings.backtrackingEnabled &&
      !state.backtracking &&
      state.latest != TimestampOffset.Zero &&
      !disableBacktrackingWhenFarBehindCurrentWallClockTime &&
      (newIdleCount >= 5 || // FIXME config?
      state.itemCountSinceBacktracking + state.itemCount >= qSettings.bufferSize * 3 ||
      JDuration
        .between(state.latestBacktracking.timestamp, state.latest.timestamp)
        .compareTo(halfBacktrackingWindow) > 0)
    }

    def nextQuery(state: QueryState): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      val newIdleCount = if (state.itemCount == 0) state.idleCount + 1 else 0
      val newIdleCountBeforeHeartbeat =
        if (state.backtracking) state.idleCountBeforeHeartbeat
        else if (state.itemCount == 0) state.idleCountBeforeHeartbeat + 1
        else 0
      // start tracking query wall clock for heartbeats after initial backtracking query
      val newQueryWallClock =
        if (state.latestBacktracking != TimestampOffset.Zero) clock.instant()
        else Instant.EPOCH

      val newState =
        if (switchToBacktracking(state, newIdleCount)) {
          // switching to backtracking
          val fromOffset =
            if (state.latestBacktracking == TimestampOffset.Zero)
              TimestampOffset(timestamp = state.latest.timestamp.minus(firstBacktrackingQueryWindow), seen = Map.empty)
            else
              state.latestBacktracking

          state.copy(
            itemCount = 0,
            itemCountSinceBacktracking = 0,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = 1,
            latestBacktracking = fromOffset,
            backtrackingExpectFiltered = state.latestBacktrackingSeenCount,
            currentQueryWallClock = newQueryWallClock,
            previousQueryWallClock = state.currentQueryWallClock,
            idleCountBeforeHeartbeat = newIdleCountBeforeHeartbeat)
        } else if (switchFromBacktracking(state)) {
          // switching from backtracking
          state.copy(
            itemCount = 0,
            itemCountSinceBacktracking = 0,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = 0,
            currentQueryWallClock = newQueryWallClock,
            previousQueryWallClock = state.currentQueryWallClock,
            idleCountBeforeHeartbeat = newIdleCountBeforeHeartbeat)
        } else {
          // continuing
          val newBacktrackingCount = if (state.backtracking) state.backtrackingCount + 1 else 0
          state.copy(
            itemCount = 0,
            itemCountSinceBacktracking = state.itemCountSinceBacktracking + state.itemCount,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = newBacktrackingCount,
            backtrackingExpectFiltered = state.latestBacktrackingSeenCount,
            currentQueryWallClock = newQueryWallClock,
            previousQueryWallClock = state.currentQueryWallClock,
            idleCountBeforeHeartbeat = newIdleCountBeforeHeartbeat)
        }

      val fromTimestamp = newState.nextQueryFromTimestamp(backtrackingWindow)
      val toTimestamp = {
        val behindCurrentTime =
          if (newState.backtracking) settings.querySettings.backtrackingBehindCurrentTime
          else settings.querySettings.behindCurrentTime
        val behindTimestamp = InstantFactory.now().minusMillis(behindCurrentTime.toMillis)
        newState.nextQueryToTimestamp match {
          case Some(t) => if (behindTimestamp.isBefore(t)) behindTimestamp else t
          case None    => behindTimestamp
        }
      }

      if (log.isDebugEnabled()) {
        val backtrackingInfo =
          if (newState.backtracking && !state.backtracking)
            s" switching to backtracking mode, [${state.itemCountSinceBacktracking + state.itemCount}] events behind,"
          else if (!newState.backtracking && state.backtracking)
            " switching from backtracking mode,"
          else if (newState.backtracking && state.backtracking)
            " in backtracking mode,"
          else
            ""
        log.debug(
          "{} next query [{}]{} from slice [{}], between time [{} - {}]. {}",
          logPrefix,
          newState.queryCount,
          backtrackingInfo,
          slice,
          fromTimestamp,
          toTimestamp,
          if (newIdleCount >= 3) s"Idle in [$newIdleCount] queries."
          else if (state.backtracking) s"Found [${state.itemCount}] items in previous backtracking query."
          else s"Found [${state.itemCount}] items in previous query.")
      }

      val newStateWithPrevious =
        if (newState.backtracking) newState.copy(previousBacktracking = newState.latestBacktracking)
        else newState.copy(previous = newState.latest)

      newStateWithPrevious ->
      Some(
        dao
          .itemsBySlice(entityType, slice, fromTimestamp, toTimestamp, backtracking = newState.backtracking)
          .filter { item =>
            filterEventsBeforeSnapshots(item.persistenceId, item.seqNr, item.source)
          }
          .via(deserializeAndAddOffset(newState.currentOffset)))
    }

    def heartbeat(state: QueryState): Option[Envelope] = {
      if (state.idleCountBeforeHeartbeat >= 2 && state.previousQueryWallClock != Instant.EPOCH) {
        // use wall clock to measure duration since start, up to idle backtracking limit
        val timestamp = state.startTimestamp.plus(
          JDuration.between(state.startWallClock, state.previousQueryWallClock.minus(backtrackingBehindCurrentTime)))

        createHeartbeat(timestamp)
      } else None
    }

    val nextHeartbeat: QueryState => Option[Envelope] =
      if (settings.journalPublishEvents) heartbeat else _ => None

    val currentTimestamp = InstantFactory.now() // Can we use DDB as a timestamp source?
    val currentWallClock = clock.instant()

    ContinuousQuery[QueryState, Envelope](
      initialState = QueryState.empty
        .copy(latest = initialOffset, startTimestamp = currentTimestamp, startWallClock = currentWallClock),
      updateState = nextOffset,
      delayNextQuery = delayNextQuery,
      nextQuery = nextQuery,
      beforeQuery = _ => None,
      heartbeat = nextHeartbeat)
  }

  private def deserializeAndAddOffset(timestampOffset: TimestampOffset): Flow[Item, Envelope, NotUsed] = {
    Flow[Item].statefulMapConcat { () =>
      var currentTimestamp = timestampOffset.timestamp
      var currentSequenceNrs: Map[String, Long] = timestampOffset.seen
      item => {
        if (item.eventTimestamp == currentTimestamp) {
          // has this already been seen?
          if (currentSequenceNrs.get(item.persistenceId).exists(_ >= item.seqNr)) {
            if (currentSequenceNrs.size >= settings.querySettings.bufferSize) {
              throw new IllegalStateException(
                s"Too many events stored with the same timestamp [$currentTimestamp], buffer size [${settings.querySettings.bufferSize}]")
            }
            log.trace(
              "filtering [{}] [{}] as db timestamp is the same as last offset and is in seen [{}]",
              item.persistenceId,
              item.seqNr,
              currentSequenceNrs)
            Nil
          } else {
            currentSequenceNrs = currentSequenceNrs.updated(item.persistenceId, item.seqNr)
            val offset =
              TimestampOffset(item.eventTimestamp, item.readTimestamp, currentSequenceNrs)
            createEnvelope(offset, item) :: Nil
          }
        } else {
          // ne timestamp, reset currentSequenceNrs
          currentTimestamp = item.eventTimestamp
          currentSequenceNrs = Map(item.persistenceId -> item.seqNr)
          val offset = TimestampOffset(item.eventTimestamp, item.readTimestamp, currentSequenceNrs)
          createEnvelope(offset, item) :: Nil
        }
      }
    }
  }
}
