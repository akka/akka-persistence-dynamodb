/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.query.scaladsl

import java.time.Clock
import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.persistence.FilteredPayload
import akka.persistence.Persistence
import akka.persistence.SerializedEvent
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.BySliceQuery
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.dynamodb.internal.PubSub
import akka.persistence.dynamodb.internal.QueryDao
import akka.persistence.dynamodb.internal.SerializedJournalItem
import akka.persistence.dynamodb.internal.SerializedSnapshotItem
import akka.persistence.dynamodb.internal.SnapshotDao
import akka.persistence.dynamodb.internal.StartingFromSnapshotStage
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffsetBySlice
import akka.persistence.query.scaladsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.annotation.nowarn
import java.util.UUID

import scala.annotation.tailrec

import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery

object DynamoDBReadJournal {
  val Identifier = "akka.persistence.dynamodb.query"
}

final class DynamoDBReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery
    with CurrentEventsBySliceStartingFromSnapshotsQuery
    with EventsBySliceStartingFromSnapshotsQuery
    with CurrentEventsByPersistenceIdTypedQuery
    with EventTimestampQuery
    with LoadEventQuery {

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = DynamoDBSettings(system.settings.config.getConfig(sharedConfigPath))
  log.debug("DynamoDB read journal starting up")

  private val typedSystem = system.toTyped
  import typedSystem.executionContext
  private val serialization = SerializationExtension(system)
  private val persistenceExt = Persistence(system)

  private val client = ClientProvider(typedSystem).clientFor(sharedConfigPath + ".client")
  private val queryDao = new QueryDao(typedSystem, settings, client)
  private val snapshotDao = new SnapshotDao(typedSystem, settings, client)

  private val filteredPayloadSerId = SerializationExtension(system).findSerializerFor(FilteredPayload).identifier

  // keyed by entity type and slice
  private val heartbeatPersistenceIds = new ConcurrentHashMap[(String, Int), String]
  private val heartbeatUuid = UUID.randomUUID().toString
  log.debug("Using heartbeat UUID [{}]", heartbeatUuid)

  private def heartbeatPersistenceId(entityType: String, slice: Int): String = {
    val key = entityType -> slice
    heartbeatPersistenceIds.get(key) match {
      case null =>
        // just a cache, don't block other threads
        val pid = generateHeartbeatPersistenceId(entityType, slice)
        heartbeatPersistenceIds.put(key, pid)
        pid

      case pid => pid
    }
  }

  @tailrec private def generateHeartbeatPersistenceId(entityType: String, slice: Int, n: Int = 1): String =
    // Inspired by Nakamoto, 2008...
    if (n < 1000000) { // a million attempts should be enough to find a match
      // UUID included to make sure not the same as any application persistence ID
      val pid = PersistenceId.concat(entityType, s"_hb-$heartbeatUuid-$n")
      if (persistenceExt.sliceForPersistenceId(pid) == slice) {
        pid
      } else {
        generateHeartbeatPersistenceId(entityType, slice, n + 1)
      }
    } else
      throw new IllegalStateException(
        s"Couldn't find a heartbeat persistence ID for [$entityType] with slice [$slice] and UUID [$heartbeatUuid]")

  private val clock = Clock.systemUTC

  private def bySlice[Event](
      entityType: String,
      slice: Int): BySliceQuery[SerializedJournalItem, EventEnvelope[Event]] = {
    val createEnvelope: (TimestampOffset, SerializedJournalItem) => EventEnvelope[Event] = createEventEnvelope
    val extractOffset = (env: EventEnvelope[Event]) => env.offset.asInstanceOf[TimestampOffset]
    val createHeartbeat = (timestamp: Instant) =>
      Some(createEventEnvelopeHeartbeat[Event](entityType, slice, timestamp))

    new BySliceQuery(queryDao, createEnvelope, extractOffset, createHeartbeat, clock, settings, log)
  }

  private def snapshotsBySlice[Snapshot, Event](
      entityType: String,
      slice: Int,
      transformSnapshot: Snapshot => Event): BySliceQuery[SerializedSnapshotItem, EventEnvelope[Event]] = {
    val createEnvelope: (TimestampOffset, SerializedSnapshotItem) => EventEnvelope[Event] =
      (offset, row) => createEnvelopeFromSnapshot(row, offset, transformSnapshot)

    val extractOffset: EventEnvelope[Event] => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]
    val createHeartbeat = (timestamp: Instant) =>
      Some(createEventEnvelopeHeartbeat[Event](entityType, slice, timestamp))

    new BySliceQuery(snapshotDao, createEnvelope, extractOffset, createHeartbeat, clock, settings, log)
  }

  private def createEnvelopeFromSnapshot[Snapshot, Event](
      item: SerializedSnapshotItem,
      offset: TimestampOffset,
      transformSnapshot: Snapshot => Event): EventEnvelope[Event] = {
    val snapshot = serialization.deserialize(item.payload, item.serId, item.serManifest).get
    val event = transformSnapshot(snapshot.asInstanceOf[Snapshot])
    val metadata = item.metadata.map(meta => serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)

    new EventEnvelope[Event](
      offset,
      item.persistenceId,
      item.seqNr,
      Option(event),
      item.eventTimestamp.toEpochMilli,
      metadata,
      PersistenceId.extractEntityType(item.persistenceId),
      persistenceExt.sliceForPersistenceId(item.persistenceId),
      filtered = false,
      source = EnvelopeOrigin.SourceSnapshot,
      tags = item.tags)
  }

  private def deserializePayload[Event](item: SerializedJournalItem): Option[Event] =
    item.payload.map(payload =>
      serialization.deserialize(payload, item.serId, item.serManifest).get.asInstanceOf[Event])

  private def deserializeBySliceItem[Event](item: SerializedJournalItem): EventEnvelope[Event] = {
    val offset = TimestampOffset(item.writeTimestamp, item.readTimestamp, Map(item.persistenceId -> item.seqNr))
    createEventEnvelope(offset, item)
  }

  private def createEventEnvelope[Event](offset: TimestampOffset, item: SerializedJournalItem): EventEnvelope[Event] = {
    val event = deserializePayload[Event](item)
    val metadata = item.metadata.map(meta => serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    val source = if (event.isDefined) EnvelopeOrigin.SourceQuery else EnvelopeOrigin.SourceBacktracking
    val filtered = item.serId == filteredPayloadSerId

    new EventEnvelope(
      offset,
      item.persistenceId,
      item.seqNr,
      if (filtered) None else event,
      item.writeTimestamp.toEpochMilli,
      metadata,
      PersistenceId.extractEntityType(item.persistenceId),
      persistenceExt.sliceForPersistenceId(item.persistenceId),
      filtered,
      source,
      tags = item.tags)
  }

  def createEventEnvelopeHeartbeat[Event](entityType: String, slice: Int, timestamp: Instant): EventEnvelope[Event] =
    new EventEnvelope(
      TimestampOffset(timestamp, Map.empty),
      heartbeatPersistenceId(entityType, slice),
      1L,
      eventOption = None,
      timestamp.toEpochMilli,
      _eventMetadata = None,
      entityType,
      slice,
      filtered = true,
      source = EnvelopeOrigin.SourceHeartbeat,
      Set.empty)

  /**
   * INTERNAL API: Used by both journal replay and currentEventsByPersistenceId
   */
  @InternalApi private[dynamodb] def internalCurrentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      includeDeleted: Boolean): Source[SerializedJournalItem, NotUsed] = {

    log.debug("[{}] eventsByPersistenceId from seqNr [{}] to [{}]", persistenceId, fromSequenceNr, toSequenceNr)

    queryDao.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, includeDeleted)
  }

  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] = {
    internalCurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, includeDeleted = false)
      .map(deserializeBySliceItem[Event])
  }

  override def sliceForPersistenceId(persistenceId: String): Int = {
    persistenceExt.sliceForPersistenceId(persistenceId)
  }

  override def sliceRanges(numberOfRanges: Int): Seq[Range] =
    persistenceExt.sliceRanges(numberOfRanges)

  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {
    val bySliceQueries = (minSlice to maxSlice).map { slice =>
      bySlice[Event](entityType, slice)
        .currentBySlice(
          s"[$entityType] currentEventsBySlice [$slice]: ",
          entityType,
          slice,
          sliceStartOffset(slice, offset))
    }
    require(bySliceQueries.nonEmpty, s"maxSlice [$maxSlice] must be >= minSlice [$minSlice]")

    // FIXME: can we replace mergeAll with a stage that does more to stagger demand?
    bySliceQueries.head.mergeAll(bySliceQueries.tail, eagerComplete = false)
  }

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
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {

    val bySliceQueries = (minSlice to maxSlice).map { slice =>
      bySlice[Event](entityType, slice).liveBySlice(
        s"[$entityType] eventsBySlice [$slice]: ",
        entityType,
        slice,
        sliceStartOffset(slice, offset))
    }
    require(bySliceQueries.nonEmpty, s"maxSlice [$maxSlice] must be >= minSlice [$minSlice]")

    val dbSource = bySliceQueries.head.mergeAll(bySliceQueries.tail, eagerComplete = false)
    if (settings.journalPublishEvents) {
      val pubSubSource = eventsBySlicesPubSubSource[Event](entityType, minSlice, maxSlice)
      mergeDbAndPubSubSources(dbSource, pubSubSource)
    } else
      dbSource
  }

  private def sliceStartOffset(slice: Int, offset: Offset): Offset = {
    offset match {
      case TimestampOffsetBySlice(offsets) => offsets.getOrElse(slice, NoOffset)
      case _                               => offset
    }
  }

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
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    checkStartFromSnapshotEnabled("currentEventsBySlicesStartingFromSnapshots")

    val bySliceQueries = (minSlice to maxSlice).map { slice =>
      val timestampOffset = TimestampOffset.toTimestampOffset(sliceStartOffset(slice, offset))

      val snapshotSource = snapshotsBySlice[Snapshot, Event](entityType, slice, transformSnapshot)
        .currentBySlice(s"[$entityType] currentSnapshotsBySlice [$slice]: ", entityType, slice, timestampOffset)

      Source.fromGraph(
        new StartingFromSnapshotStage[Event](
          snapshotSource,
          { snapshotOffsets =>
            val initOffset =
              if (timestampOffset == TimestampOffset.Zero && snapshotOffsets.nonEmpty) {
                val minTimestamp = snapshotOffsets.valuesIterator.minBy { case (_, timestamp) => timestamp }._2
                TimestampOffset(minTimestamp, Map.empty)
              } else {
                // don't adjust because then there is a risk that there was no found snapshot for a persistenceId
                // but there can still be events between the given `offset` parameter and the min timestamp of the
                // snapshots and those would then be missed
                offset
              }

            log.debug(
              "currentEventsBySlicesStartingFromSnapshots for slice [{}] and initOffset [{}] with [{}] snapshots",
              slice,
              initOffset,
              snapshotOffsets.size)

            bySlice[Event](entityType, slice).currentBySlice(
              s"[$entityType] currentEventsBySlice [$slice]: ",
              entityType,
              slice,
              initOffset,
              filterEventsBeforeSnapshots(snapshotOffsets, backtrackingEnabled = false))
          }))
    }

    require(bySliceQueries.nonEmpty, s"maxSlice [$maxSlice] must be >= minSlice [$minSlice]")

    bySliceQueries.head.mergeAll(bySliceQueries.tail, eagerComplete = false)
  }

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
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    checkStartFromSnapshotEnabled("eventsBySlicesStartingFromSnapshots")

    val bySliceQueries = (minSlice to maxSlice).map { slice =>
      val timestampOffset = TimestampOffset.toTimestampOffset(sliceStartOffset(slice, offset))

      val snapshotSource = snapshotsBySlice[Snapshot, Event](entityType, slice, transformSnapshot)
        .currentBySlice(s"[$entityType] snapshotsBySlice [$slice]: ", entityType, slice, timestampOffset)

      Source.fromGraph(
        new StartingFromSnapshotStage[Event](
          snapshotSource,
          { snapshotOffsets =>
            val initOffset =
              if (timestampOffset == TimestampOffset.Zero && snapshotOffsets.nonEmpty) {
                val minTimestamp = snapshotOffsets.valuesIterator.minBy { case (_, timestamp) => timestamp }._2
                TimestampOffset(minTimestamp, Map.empty)
              } else {
                // don't adjust because then there is a risk that there was no found snapshot for a persistenceId
                // but there can still be events between the given `offset` parameter and the min timestamp of the
                // snapshots and those would then be missed
                offset
              }

            log.debug(
              "eventsBySlicesStartingFromSnapshots for slice [{}] and initOffset [{}] with [{}] snapshots",
              slice,
              initOffset,
              snapshotOffsets.size)

            bySlice[Event](entityType, slice).liveBySlice(
              s"[$entityType] eventsBySlice [$slice]: ",
              entityType,
              slice,
              initOffset,
              filterEventsBeforeSnapshots(snapshotOffsets, settings.querySettings.backtrackingEnabled))
          }))
    }

    require(bySliceQueries.nonEmpty, s"maxSlice [$maxSlice] must be >= minSlice [$minSlice]")

    val dbSource = bySliceQueries.head.mergeAll(bySliceQueries.tail, eagerComplete = false)
    if (settings.journalPublishEvents) {
      val pubSubSource = eventsBySlicesPubSubSource[Event](entityType, minSlice, maxSlice)
      mergeDbAndPubSubSources(dbSource, pubSubSource)
    } else
      dbSource
  }

  // Stateful filter function that decides if (persistenceId, seqNr, source) should be emitted by
  // `eventsBySlicesStartingFromSnapshots` and `currentEventsBySlicesStartingFromSnapshots`.
  private def filterEventsBeforeSnapshots(
      snapshotOffsets: Map[String, (Long, Instant)],
      backtrackingEnabled: Boolean): (String, Long, String) => Boolean = {
    var _snapshotOffsets = snapshotOffsets
    (persistenceId, seqNr, source) => {
      if (_snapshotOffsets.isEmpty)
        true
      else
        _snapshotOffsets.get(persistenceId) match {
          case None                     => true
          case Some((snapshotSeqNr, _)) =>
            //  release memory by removing from the _snapshotOffsets Map
            if (seqNr == snapshotSeqNr &&
              ((backtrackingEnabled && source == EnvelopeOrigin.SourceBacktracking) ||
              (!backtrackingEnabled && source == EnvelopeOrigin.SourceQuery))) {
              _snapshotOffsets -= persistenceId
            }

            seqNr > snapshotSeqNr
        }
    }
  }

  private def checkStartFromSnapshotEnabled(methodName: String): Unit =
    if (!settings.querySettings.startFromSnapshotEnabled)
      throw new IllegalArgumentException(
        s"To use $methodName you must enable configuration `akka.persistence.dynamodb.query.start-from-snapshot.enabled`")

  @nowarn("msg=deprecated")
  private def eventsBySlicesPubSubSource[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int): Source[EventEnvelope[Event], NotUsed] = {
    val pubSub = PubSub(typedSystem)
    Source
      .actorRef[EventEnvelope[Event]](
        completionMatcher = PartialFunction.empty,
        failureMatcher = PartialFunction.empty,
        bufferSize = settings.querySettings.bufferSize,
        overflowStrategy = OverflowStrategy.dropNew)
      .mapMaterializedValue { ref =>
        pubSub.eventTopics[Event](entityType, minSlice, maxSlice).foreach { topic =>
          import akka.actor.typed.scaladsl.adapter._
          topic ! Topic.Subscribe(ref.toTyped[EventEnvelope[Event]])
        }
      }
      .filter { env =>
        val slice = sliceForPersistenceId(env.persistenceId)
        minSlice <= slice && slice <= maxSlice
      }
      .map { env =>
        env.eventOption match {
          case Some(se: SerializedEvent) =>
            env.withEvent(deserializeEvent(se))
          case _ => env
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def deserializeEvent[Event](se: SerializedEvent): Event =
    serialization.deserialize(se.bytes, se.serializerId, se.serializerManifest).get.asInstanceOf[Event]

  private def mergeDbAndPubSubSources[Event, Snapshot](
      dbSource: Source[EventEnvelope[Event], NotUsed],
      pubSubSource: Source[EventEnvelope[Event], NotUsed]) = {
    dbSource
      .mergePrioritized(pubSubSource, leftPriority = 1, rightPriority = 10)
      .via(
        skipPubSubTooFarAhead(
          settings.querySettings.backtrackingEnabled,
          JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)))
      .via(deduplicate(settings.querySettings.deduplicateCapacity))
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def deduplicate[Event](
      capacity: Int): Flow[EventEnvelope[Event], EventEnvelope[Event], NotUsed] = {
    if (capacity == 0)
      Flow[EventEnvelope[Event]]
    else {
      val evictThreshold = (capacity * 1.1).toInt
      Flow[EventEnvelope[Event]]
        .statefulMapConcat(() => {
          // cache of seen pid/seqNr
          var seen = mutable.LinkedHashSet.empty[(String, Long)]
          env => {
            if (EnvelopeOrigin.fromBacktracking(env)) {
              // don't deduplicate from backtracking
              env :: Nil
            } else {
              val entry = env.persistenceId -> env.sequenceNr
              val result = {
                if (seen.contains(entry)) {
                  Nil
                } else {
                  seen.add(entry)
                  env :: Nil
                }
              }

              if (seen.size >= evictThreshold) {
                // weird that add modifies the instance but drop returns a new instance
                seen = seen.drop(seen.size - capacity)
              }

              result
            }
          }
        })
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def skipPubSubTooFarAhead[Event](
      enabled: Boolean,
      maxAheadOfBacktracking: JDuration): Flow[EventEnvelope[Event], EventEnvelope[Event], NotUsed] = {
    if (!enabled)
      Flow[EventEnvelope[Event]]
    else
      Flow[EventEnvelope[Event]]
        .statefulMapConcat(() => {
          // track backtracking offset per slice
          var latestBacktrackingPerSlice = Map.empty[Int, Instant]
          def latestBacktracking(slice: Int): Instant = latestBacktrackingPerSlice.get(slice) match {
            case Some(instant) => instant
            case None          => Instant.EPOCH
          }
          env => {
            val slice = persistenceExt.sliceForPersistenceId(env.persistenceId)
            env.offset match {
              case t: TimestampOffset =>
                if (EnvelopeOrigin.fromQuery(env)) {
                  if (log.isDebugEnabled()) {
                    val l = latestBacktracking(slice)
                    if (l.isAfter(t.timestamp))
                      log.debug(
                        "event from query for persistenceId [{}] seqNr [{}] " +
                        s"timestamp [{}] was before last event from backtracking or heartbeat [{}].",
                        env.persistenceId,
                        env.sequenceNr,
                        t.timestamp,
                        l)
                  }

                  env :: Nil
                } else {
                  if (EnvelopeOrigin.fromBacktracking(env)) {
                    latestBacktrackingPerSlice = latestBacktrackingPerSlice.updated(slice, t.timestamp)
                    env :: Nil
                  } else if (EnvelopeOrigin.fromHeartbeat(env)) {
                    latestBacktrackingPerSlice = latestBacktrackingPerSlice.updated(slice, t.timestamp)
                    Nil // always drop heartbeats
                  } else if (EnvelopeOrigin.fromPubSub(env) && latestBacktracking(slice) == Instant.EPOCH) {
                    log.trace(
                      "Dropping pubsub event for persistenceId [{}] seqNr [{}] because no event from backtracking yet.",
                      env.persistenceId,
                      env.sequenceNr)
                    Nil
                  } else if (EnvelopeOrigin.fromPubSub(env) && JDuration
                      .between(latestBacktracking(slice), t.timestamp)
                      .compareTo(maxAheadOfBacktracking) > 0) {
                    // drop from pubsub when too far ahead from backtracking
                    log.debug(
                      "Dropping pubsub event for persistenceId [{}] seqNr [{}] because too far ahead of backtracking.",
                      env.persistenceId,
                      env.sequenceNr)
                    Nil
                  } else {
                    env :: Nil
                  }
                }
              case _ =>
                env :: Nil
            }
          }
        })
  }

  // EventTimestampQuery
  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    val result = queryDao.timestampOfEvent(persistenceId, sequenceNr)
    if (log.isDebugEnabled) {
      result.foreach { t =>
        log.debug("[{}] timestampOf seqNr [{}] is [{}]", persistenceId, sequenceNr, t)
      }
    }
    result
  }

  //LoadEventQuery
  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
    log.debug("[{}] loadEnvelope seqNr [{}]", persistenceId, sequenceNr)
    queryDao
      .loadEvent(persistenceId, sequenceNr, includePayload = true)
      .map {
        case Some(item) => deserializeBySliceItem(item)
        case None =>
          throw new NoSuchElementException(
            s"Event with persistenceId [$persistenceId] and sequenceNr [$sequenceNr] not found.")
      }
  }

}
