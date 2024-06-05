/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.query.scaladsl

import java.time.Instant

import scala.concurrent.Future

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.persistence.FilteredPayload
import akka.persistence.Persistence
import akka.persistence.dynamodb.ClientProvider
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.BySliceQuery
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.dynamodb.internal.QueryDao
import akka.persistence.dynamodb.internal.SerializedJournalItem
import akka.persistence.dynamodb.internal.TimestampOffsetBySlice
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.scaladsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object DynamoDBReadJournal {
  val Identifier = "akka.persistence.dynamodb.query"
}

final class DynamoDBReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery
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

  private val filteredPayloadSerId = SerializationExtension(system).findSerializerFor(FilteredPayload).identifier

  private val _bySlice: BySliceQuery[SerializedJournalItem, EventEnvelope[Any]] = {
    val createEnvelope: (TimestampOffset, SerializedJournalItem) => EventEnvelope[Any] = createEventEnvelope

    val extractOffset: EventEnvelope[Any] => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]

    new BySliceQuery(queryDao, createEnvelope, extractOffset, settings, log)
  }

  private def bySlice[Event]: BySliceQuery[SerializedJournalItem, EventEnvelope[Event]] =
    _bySlice.asInstanceOf[BySliceQuery[SerializedJournalItem, EventEnvelope[Event]]]

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

  /**
   * INTERNAL API: Used by both journal replay and currentEventsByPersistenceId
   */
  @InternalApi private[dynamodb] def internalCurrentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalItem, NotUsed] = {

    queryDao.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
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
      bySlice[Event].currentBySlice("currentEventsBySlices", entityType, slice, sliceStartOffset(slice, offset))
    }
    require(bySliceQueries.nonEmpty, s"maxSlice [$maxSlice] must be >= minSlice [$minSlice]")
    bySliceQueries.head.mergeAll(bySliceQueries.tail, eagerComplete = false)
  }

  /**
   * Query events for given slices. A slice is deterministically defined based on the persistence id. The purpose is to
   * evenly distribute all persistence ids over the slices.
   *
   * The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
   * query from a given `offset` after a crash/restart.
   *
   * The supported offset is [[TimestampOffset]] and [[Offset.noOffset]].
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

      bySlice[Event].liveBySlice("eventsBySlices", entityType, slice, sliceStartOffset(slice, offset))
    }
    require(bySliceQueries.nonEmpty, s"maxSlice [$maxSlice] must be >= minSlice [$minSlice]")

    val dbSource = bySliceQueries.head.mergeAll(bySliceQueries.tail, eagerComplete = false)

    // FIXME merge with pubSubSource
    dbSource
  }

  private def sliceStartOffset(slice: Int, offset: Offset): Offset = {
    offset match {
      case offsetBySlice: TimestampOffsetBySlice => offsetBySlice.offsets.getOrElse(slice, NoOffset)
      case _                                     => offset
    }
  }

  // EventTimestampQuery
  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    queryDao.timestampOfEvent(persistenceId, sequenceNr)
  }

  //LoadEventQuery
  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
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
