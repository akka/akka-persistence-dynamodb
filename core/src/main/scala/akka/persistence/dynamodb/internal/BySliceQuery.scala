/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.actor.typed.scaladsl.LoggerOps
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
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  object QueryState {
    val empty: QueryState =
      QueryState(TimestampOffset.Zero, 0, 0, 0, 0, backtrackingCount = 0, TimestampOffset.Zero, 0, 0)
  }

  final case class QueryState(
      latest: TimestampOffset,
      rowCount: Int,
      rowCountSinceBacktracking: Long,
      queryCount: Long,
      idleCount: Long,
      backtrackingCount: Int,
      latestBacktracking: TimestampOffset,
      latestBacktrackingSeenCount: Int,
      backtrackingExpectFiltered: Int) {

    def backtracking: Boolean = backtrackingCount > 0

    def currentOffset: TimestampOffset =
      if (backtracking) latestBacktracking
      else latest

    def nextQueryFromTimestamp: Instant =
      if (backtracking) latestBacktracking.timestamp
      else latest.timestamp

    def nextQueryToTimestamp: Option[Instant] = {
      if (backtracking) Some(latest.timestamp)
      else None
    }
  }

  trait SerializedRow {
    def persistenceId: String
    def seqNr: Long
    def writeTimestamp: Instant
    def readTimestamp: Instant
    def source: String
  }

  trait Dao[SerializedRow] {
    def rowsBySlices(
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        fromTimestamp: Instant,
        toTimestamp: Option[Instant],
        behindCurrentTime: FiniteDuration,
        backtracking: Boolean): Source[SerializedRow, NotUsed]

  }
}

/**
 * INTERNAL API
 */
@InternalApi private[dynamodb] class BySliceQuery[Row <: BySliceQuery.SerializedRow, Envelope](
    dao: BySliceQuery.Dao[Row],
    createEnvelope: (TimestampOffset, Row) => Envelope,
    extractOffset: Envelope => TimestampOffset,
    settings: DynamoDBSettings,
    log: Logger)(implicit val ec: ExecutionContext) {
  import BySliceQuery._
  import TimestampOffset.toTimestampOffset

  private val backtrackingWindow = JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)
  private val halfBacktrackingWindow = backtrackingWindow.dividedBy(2)
  private val firstBacktrackingQueryWindow =
    backtrackingWindow.plus(JDuration.ofMillis(settings.querySettings.backtrackingBehindCurrentTime.toMillis))

  def currentBySlices(
      logPrefix: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      filterEventsBeforeSnapshots: (String, Long, String) => Boolean = (_, _, _) => true): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    def nextOffset(state: QueryState, envelope: Envelope): QueryState =
      state.copy(latest = extractOffset(envelope), rowCount = state.rowCount + 1)

    def nextQuery(state: QueryState, endTimestamp: Instant): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      // Note that we can't know how many events with the same timestamp that are filtered out
      // so continue until rowCount is 0. That means an extra query at the end to make sure there are no
      // more to fetch.
      if (state.queryCount == 0L || state.rowCount > 0) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        val toTimestamp = newState.nextQueryToTimestamp match {
          case Some(t) =>
            if (t.isBefore(endTimestamp)) t else endTimestamp
          case None =>
            endTimestamp
        }

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debugN(
            "{} next query [{}] from slices [{} - {}], between time [{} - {}]. Found [{}] rows in previous query.",
            logPrefix,
            state.queryCount,
            minSlice,
            maxSlice,
            state.latest.timestamp,
            toTimestamp,
            state.rowCount)

        newState -> Some(
          dao
            .rowsBySlices(
              entityType,
              minSlice,
              maxSlice,
              state.latest.timestamp,
              toTimestamp = Some(toTimestamp),
              behindCurrentTime = Duration.Zero,
              backtracking = false)
            .filter { row =>
              filterEventsBeforeSnapshots(row.persistenceId, row.seqNr, row.source)
            }
            .via(deserializeAndAddOffset(state.latest)))
      } else {
        if (log.isDebugEnabled)
          log.debugN(
            "{} query [{}] from slices [{} - {}] completed. Found [{}] rows in previous query.",
            logPrefix,
            state.queryCount,
            minSlice,
            maxSlice,
            state.rowCount)

        state -> None
      }
    }

    val currentTimestamp = InstantFactory.now()
    if (log.isDebugEnabled())
      log.debugN(
        "{} query slices [{} - {}], from time [{}] until now [{}].",
        logPrefix,
        minSlice,
        maxSlice,
        initialOffset.timestamp,
        currentTimestamp)

    ContinuousQuery[QueryState, Envelope](
      initialState = QueryState.empty.copy(latest = initialOffset),
      updateState = nextOffset,
      delayNextQuery = _ => None,
      nextQuery = state => nextQuery(state, currentTimestamp),
      beforeQuery = _ => None)

  }

  def liveBySlices(
      logPrefix: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      filterEventsBeforeSnapshots: (String, Long, String) => Boolean = (_, _, _) => true): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    if (log.isDebugEnabled())
      log.debugN(
        "Starting {} query from slices [{} - {}], from time [{}].",
        logPrefix,
        minSlice,
        maxSlice,
        initialOffset.timestamp)

    def nextOffset(state: QueryState, envelope: Envelope): QueryState = {
      val offset = extractOffset(envelope)
      if (state.backtracking) {
        if (offset.timestamp.isBefore(state.latestBacktracking.timestamp))
          throw new IllegalArgumentException(
            s"Unexpected offset [$offset] before latestBacktracking [${state.latestBacktracking}].")

        val newSeenCount =
          if (offset.timestamp == state.latestBacktracking.timestamp) state.latestBacktrackingSeenCount + 1 else 1

        state.copy(
          latestBacktracking = offset,
          latestBacktrackingSeenCount = newSeenCount,
          rowCount = state.rowCount + 1)

      } else {
        if (offset.timestamp.isBefore(state.latest.timestamp))
          throw new IllegalArgumentException(s"Unexpected offset [$offset] before latest [${state.latest}].")

        if (log.isDebugEnabled()) {
          if (state.latestBacktracking.seen.nonEmpty &&
            offset.timestamp.isAfter(state.latestBacktracking.timestamp.plus(firstBacktrackingQueryWindow)))
            log.debugN(
              "{} next offset is outside the backtracking window, latestBacktracking: [{}], offset: [{}]",
              logPrefix,
              state.latestBacktracking,
              offset)
        }

        state.copy(latest = offset, rowCount = state.rowCount + 1)
      }
    }

    def delayNextQuery(state: QueryState): Option[FiniteDuration] = {
      if (switchFromBacktracking(state)) {
        // switch from from backtracking immediately
        None
      } else {
        val delay = ContinuousQuery.adjustNextDelay(
          state.rowCount,
          settings.querySettings.bufferSize,
          settings.querySettings.refreshInterval)

        if (log.isDebugEnabled)
          delay.foreach { d =>
            log.debugN(
              "{} query [{}] from slices [{} - {}] delay next [{}] ms.",
              logPrefix,
              state.queryCount,
              minSlice,
              maxSlice,
              d.toMillis)
          }

        delay
      }
    }

    def switchFromBacktracking(state: QueryState): Boolean = {
      state.backtracking && state.rowCount < settings.querySettings.bufferSize - state.backtrackingExpectFiltered
    }

    def nextQuery(state: QueryState): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      val newIdleCount = if (state.rowCount == 0) state.idleCount + 1 else 0
      val newState =
        if (settings.querySettings.backtrackingEnabled && !state.backtracking && state.latest != TimestampOffset.Zero &&
          (newIdleCount >= 5 ||
          state.rowCountSinceBacktracking + state.rowCount >= settings.querySettings.bufferSize * 3 ||
          JDuration
            .between(state.latestBacktracking.timestamp, state.latest.timestamp)
            .compareTo(halfBacktrackingWindow) > 0)) {
          // FIXME config for newIdleCount >= 5 and maybe something like `newIdleCount % 5 == 0`

          // Note that when starting the query with offset = NoOffset it will switch to backtracking immediately after
          // the first normal query because between(latestBacktracking.timestamp, latest.timestamp) > halfBacktrackingWindow

          // switching to backtracking
          val fromOffset =
            if (state.latestBacktracking == TimestampOffset.Zero)
              TimestampOffset(timestamp = state.latest.timestamp.minus(firstBacktrackingQueryWindow), seen = Map.empty)
            else
              state.latestBacktracking

          state.copy(
            rowCount = 0,
            rowCountSinceBacktracking = 0,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = 1,
            latestBacktracking = fromOffset,
            backtrackingExpectFiltered = state.latestBacktrackingSeenCount)
        } else if (switchFromBacktracking(state)) {
          // switch from backtracking
          state.copy(
            rowCount = 0,
            rowCountSinceBacktracking = 0,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = 0)
        } else {
          // continue
          val newBacktrackingCount = if (state.backtracking) state.backtrackingCount + 1 else 0
          state.copy(
            rowCount = 0,
            rowCountSinceBacktracking = state.rowCountSinceBacktracking + state.rowCount,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = newBacktrackingCount,
            backtrackingExpectFiltered = state.latestBacktrackingSeenCount)
        }

      val behindCurrentTime =
        if (newState.backtracking) settings.querySettings.backtrackingBehindCurrentTime
        else settings.querySettings.behindCurrentTime

      val fromTimestamp = newState.nextQueryFromTimestamp
      val toTimestamp = newState.nextQueryToTimestamp

      if (log.isDebugEnabled()) {
        val backtrackingInfo =
          if (newState.backtracking && !state.backtracking)
            s" switching to backtracking mode, [${state.rowCountSinceBacktracking + state.rowCount}] events behind,"
          else if (!newState.backtracking && state.backtracking)
            " switching from backtracking mode,"
          else if (newState.backtracking && state.backtracking)
            " in backtracking mode,"
          else
            ""
        log.debugN(
          "{} next query [{}]{} from slices [{} - {}], between time [{} - {}]. {}",
          logPrefix,
          newState.queryCount,
          backtrackingInfo,
          minSlice,
          maxSlice,
          fromTimestamp,
          toTimestamp.getOrElse("None"),
          if (newIdleCount >= 3) s"Idle in [$newIdleCount] queries."
          else if (state.backtracking) s"Found [${state.rowCount}] rows in previous backtracking query."
          else s"Found [${state.rowCount}] rows in previous query.")
      }

      newState ->
      Some(
        dao
          .rowsBySlices(
            entityType,
            minSlice,
            maxSlice,
            fromTimestamp,
            toTimestamp,
            behindCurrentTime,
            backtracking = newState.backtracking)
          .filter { row =>
            filterEventsBeforeSnapshots(row.persistenceId, row.seqNr, row.source)
          }
          .via(deserializeAndAddOffset(newState.currentOffset)))
    }

    ContinuousQuery[QueryState, Envelope](
      initialState = QueryState.empty.copy(latest = initialOffset),
      updateState = nextOffset,
      delayNextQuery = delayNextQuery,
      nextQuery = nextQuery,
      beforeQuery = _ => None)
  }

  private def deserializeAndAddOffset(timestampOffset: TimestampOffset): Flow[Row, Envelope, NotUsed] = {
    Flow[Row].statefulMapConcat { () =>
      var currentTimestamp = timestampOffset.timestamp
      var currentSequenceNrs: Map[String, Long] = timestampOffset.seen
      row => {
        if (row.writeTimestamp == currentTimestamp) {
          // has this already been seen?
          if (currentSequenceNrs.get(row.persistenceId).exists(_ >= row.seqNr)) {
            if (currentSequenceNrs.size >= settings.querySettings.bufferSize) {
              throw new IllegalStateException(
                s"Too many events stored with the same timestamp [$currentTimestamp], buffer size [${settings.querySettings.bufferSize}]")
            }
            log.traceN(
              "filtering [{}] [{}] as db timestamp is the same as last offset and is in seen [{}]",
              row.persistenceId,
              row.seqNr,
              currentSequenceNrs)
            Nil
          } else {
            currentSequenceNrs = currentSequenceNrs.updated(row.persistenceId, row.seqNr)
            val offset =
              TimestampOffset(row.writeTimestamp, row.readTimestamp, currentSequenceNrs)
            createEnvelope(offset, row) :: Nil
          }
        } else {
          // ne timestamp, reset currentSequenceNrs
          currentTimestamp = row.writeTimestamp
          currentSequenceNrs = Map(row.persistenceId -> row.seqNr)
          val offset = TimestampOffset(row.writeTimestamp, row.readTimestamp, currentSequenceNrs)
          createEnvelope(offset, row) :: Nil
        }
      }
    }
  }
}
