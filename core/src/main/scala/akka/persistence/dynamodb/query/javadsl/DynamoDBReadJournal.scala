/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.query.javadsl

import java.time.Instant
import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters.RichOption

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.japi.Pair
import akka.persistence.dynamodb.query.scaladsl
import akka.persistence.query.Offset
import akka.persistence.query.javadsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.javadsl.CurrentEventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.EventsBySliceQuery
import akka.persistence.query.typed.javadsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.stream.javadsl.Source

object DynamoDBReadJournal {
  val Identifier: String = scaladsl.DynamoDBReadJournal.Identifier
}

final class DynamoDBReadJournal(delegate: scaladsl.DynamoDBReadJournal)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery
    with CurrentEventsBySliceStartingFromSnapshotsQuery
    with EventsBySliceStartingFromSnapshotsQuery
    with EventTimestampQuery
    with LoadEventQuery {

  override def sliceForPersistenceId(persistenceId: String): Int =
    delegate.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): util.List[Pair[Integer, Integer]] = {
    delegate
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }

  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    delegate.currentEventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  /**
   * Query events for given slices. A slice is deterministically defined based on the persistence id. The purpose is to
   * evenly distribute all persistence ids over the slices.
   *
   * The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
   * query from a given `offset` after a crash/restart.
   *
   * The supported offsets are [[akka.persistence.query.TimestampOffset]] and [[akka.persistence.query.NoOffset]].
   *
   * The timestamp is based on the client wall clock and the events are read from a DynamoDB global secondary index,
   * which is eventually consistent. This means that a "later" event may be visible first and when retrieving events
   * after the previously seen timestamp we may miss some events. For that reason it will perform additional
   * backtracking queries to catch missed events. Events from backtracking will typically be duplicates of previously
   * emitted events. It's the responsibility of the consumer to filter duplicates and make sure that events are
   * processed in exact sequence number order for each persistence id. Such deduplication is provided by the DynamoDB
   * Projection.
   *
   * Events emitted by the backtracking don't contain the event payload (`EventBySliceEnvelope.event` is None) and the
   * consumer can load the full `EventBySliceEnvelope` with [[DynamoDBReadJournal.loadEnvelope]].
   *
   * The events will be emitted in the timestamp order with the caveat of duplicate events as described above.
   *
   * The stream is not completed when it reaches the end of the currently stored events, but it continues to push new
   * events when new events are persisted. Corresponding query that is completed when it reaches the end of the
   * currently stored events is provided by [[DynamoDBReadJournal.currentEventsBySlices]].
   */
  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  /**
   * Same as `currentEventsBySlices` but with the purpose to use snapshots as starting points and thereby reducing the
   * number of events that have to be loaded. This can be useful if the consumer starts from zero without any previously
   * processed offset or if it has been disconnected for a long while and its offset is far behind.
   *
   * First it loads all snapshots with timestamps greater than or equal to the offset timestamp. There is at most one
   * snapshot per persistenceId. The snapshots are transformed to events with the given `transformSnapshot` function.
   *
   * After emitting the snapshot events the ordinary events with sequence numbers after the snapshots are emitted.
   *
   * To use `currentEventsBySlicesStartingFromSnapshots` you must enable configuration
   * `akka.persistence.dynamodb.query.start-from-snapshot.enabled`.
   */
  override def currentEventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: java.util.function.Function[Snapshot, Event]): Source[EventEnvelope[Event], NotUsed] =
    delegate
      .currentEventsBySlicesStartingFromSnapshots(entityType, minSlice, maxSlice, offset, transformSnapshot(_))
      .asJava

  /**
   * Same as `eventsBySlices` but with the purpose to use snapshots as starting points and thereby reducing the number
   * of events that have to be loaded. This can be useful if the consumer starts from zero without any previously
   * processed offset or if it has been disconnected for a long while and its offset is far behind.
   *
   * First it loads all snapshots with timestamps greater than or equal to the offset timestamp. There is at most one
   * snapshot per persistenceId. The snapshots are transformed to events with the given `transformSnapshot` function.
   *
   * After emitting the snapshot events the ordinary events with sequence numbers after the snapshots are emitted.
   *
   * To use `eventsBySlicesStartingFromSnapshots` you must enable configuration
   * `akka.persistence.dynamodb.query.start-from-snapshot.enabled`.
   */
  override def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: java.util.function.Function[Snapshot, Event]): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsBySlicesStartingFromSnapshots(entityType, minSlice, maxSlice, offset, transformSnapshot(_)).asJava

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    delegate.timestampOf(persistenceId, sequenceNr).map(_.toJava)(ExecutionContexts.parasitic).asJava

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    delegate.loadEnvelope[Event](persistenceId, sequenceNr).asJava
}
