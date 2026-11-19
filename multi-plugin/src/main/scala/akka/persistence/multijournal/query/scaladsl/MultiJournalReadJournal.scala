/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal.query.scaladsl

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.jdk.DurationConverters.JavaDurationOps
import scala.jdk.FutureConverters.CompletionStageOps
import scala.jdk.OptionConverters.RichOptional
import scala.util.control.NoStackTrace
import scala.util.control.NonFatal

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.Persistence
import akka.persistence.multijournal.query.internal.MergeHotWithCoolerStage
import akka.persistence.multijournal.MultiJournalMetadata
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffsetBySlice
import akka.persistence.query.javadsl.{ ReadJournal => JavaReadJournal }
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.{ javadsl => javaQuery }
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.stream.SourceShape
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object MultiJournalReadJournal {
  val Identifier = "akka.persistence.multi-journal.query"
}

final class MultiJournalReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery
    with CurrentEventsByPersistenceIdTypedQuery
    with EventTimestampQuery
    with LoadEventQuery {
  private val log = LoggerFactory.getLogger(getClass)
  private val persistenceQueryExt = PersistenceQuery(system)
  private val persistenceExt = Persistence(system)

  private final class ReadJournalPair(val scala: ReadJournal, val java: JavaReadJournal)

  private def getReadJournals(): Map[Int, ReadJournalPair] = {
    val cfg = config.getObject("read-journals")
    val (minJournal, maxJournal) = cfg.keySet.asScala.foldLeft(Int.MaxValue -> -1) {
      case ((minJournalId, maxJournalId), candidate) =>
        val kOpt = candidate.toIntOption
        require(kOpt.exists(_ >= 0), s"Read-journal keys must be non-negative integers, got [$candidate]")

        val cfgVal = cfg.get(candidate)
        cfgVal.unwrapped match {
          case _: String =>
            val k = kOpt.get
            minJournalId.min(k) -> maxJournalId.max(k)

          case _ =>
            throw new IllegalArgumentException(s"Read-journal values must be strings, got [$cfgVal] for key $candidate")
        }
    }
    require(maxJournal >= minJournal, "At least one journal must be specified")
    val overallConfig = system.settings.config
    val pluginConfig = config

    (minJournal to maxJournal).iterator.map { key =>
      val path = pluginConfig.getString(s"read-journals.$key")
      require(overallConfig.hasPath(path), s"Read-journal values must be valid config paths, got [$path] for $key")

      val scalaReadJournal = persistenceQueryExt.readJournalFor[ReadJournal](path)
      val javaReadJournal = persistenceQueryExt.getReadJournalFor(classOf[JavaReadJournal], path)
      key -> new ReadJournalPair(scalaReadJournal, javaReadJournal)
    }.toMap
  }

  private val readJournals =
    try { getReadJournals() }
    catch { case NonFatal(ex) => log.error("Could not start read journal for multi-journal plugin"); throw ex }

  val journalRange = {
    val (min, max) = readJournals.keys.foldLeft(Int.MaxValue -> -1) { case ((min, max), id) =>
      (min.min(id), max.max(id))
    }
    min to max
  }

  private val coldJournalBehindCurrentTime = config.getDuration("cold-journal-behind-current-time").toScala
  private val queryTimeWindow = config.getDuration("query-time-window").toScala
  require(
    queryTimeWindow > coldJournalBehindCurrentTime,
    s"The query-time-window must be greater than the cold-journal-behind-current-time, was [$queryTimeWindow] <= [$coldJournalBehindCurrentTime]")

  private lazy val CurrentEventsByPersistenceIdQueries =
    allSupport("Current events by persistence ID typed query")(currentEventsByPersistenceIdQuery)

  private lazy val EventTimestampQueries = allSupport("Event timestamp query")(eventTimestampQuery)
  private lazy val LoadEventQueries = allSupport("Load event query")(loadEventQuery)
  private lazy val CurrentEventsBySliceQueries = allSupport("Current events by slice")(currentEventsBySliceQuery)
  private lazy val EventsBySliceQueries = allSupport("Events by slice")(eventsBySliceQuery)
  private lazy val TimestampOffsetRequired = new IllegalArgumentException(
    "Offsets must be NoOffset, TimestampOffset, or TimestampOffsetBySlice") with NoStackTrace

  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
    CurrentEventsByPersistenceIdQueries match {
      case Right(queries) =>
        Source(journalRange)
          .map(queries)
          .flatMapConcat {
            _.currentEventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr)
          }
          .map(_.removeMetadata[MultiJournalMetadata])

      case Left(ex) => Source.failed(ex)
    }

  override def sliceForPersistenceId(persistenceId: String): Int = persistenceExt.sliceForPersistenceId(persistenceId)
  override def sliceRanges(numberOfRanges: Int): Seq[Range] = persistenceExt.sliceRanges(numberOfRanges)

  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    CurrentEventsBySliceQueries match {
      case Right(queries) =>
        require(maxSlice >= minSlice, "maxSlice must be at least minSlice")
        requireTimestampOffset(offset)

        val numSlices = 1 + maxSlice - minSlice
        Source
          .fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
            import GraphDSL.Implicits._

            val merge = builder.add(new Merge[EventEnvelope[Event]](numSlices, eagerComplete = false))
            (minSlice to maxSlice).iterator.zipWithIndex.foreach { case (slice, i) =>
              unsafeCurrentEventsForSingleSlice[Event](entityType, slice, offset, queries) ~> merge.in(i)
            }

            SourceShape(merge.out)
          })
          .map(_.removeMetadata[MultiJournalMetadata])

      case Left(ex) => Source.failed(ex)
    }

  private def requireTimestampOffset(offset: Offset): Unit =
    offset match {
      case NoOffset                  => // good
      case _: TimestampOffset        => // good
      case _: TimestampOffsetBySlice => // also good
      case _                         => throw TimestampOffsetRequired
    }

  private def unsafeCurrentEventsForSingleSlice[Event](
      entityType: String,
      slice: Int,
      offset: Offset,
      queries: Map[Int, CurrentEventsBySliceQuery]): Source[EventEnvelope[Event], NotUsed] =
    unsafeCurrentEventsForSingleSlice(entityType, slice, offset, journalRange.min, journalRange.max, queries)

  private def unsafeCurrentEventsForSingleSlice[Event](
      entityType: String,
      slice: Int,
      offset: Offset,
      minJournal: Int,
      maxJournal: Int,
      queries: Map[Int, CurrentEventsBySliceQuery]): Source[EventEnvelope[Event], NotUsed] = {
    def baseQueryFor(journal: Int): Source[EventEnvelope[Event], NotUsed] =
      queries(journal).currentEventsBySlices(entityType, slice, slice, offset)

    def mergeTree(min: Int, max: Int): Source[EventEnvelope[Event], NotUsed] = {
      implicit val ord: Ordering[EventEnvelope[Event]] =
        Ordering.by { envelope =>
          envelope.offset match {
            case TimestampOffset(timestamp, _, _) => timestamp
            case _                                => Instant.ofEpochMilli(envelope.timestamp)
          }
        }

      if (min == max) baseQueryFor(min)
      else if (max == min + 1) baseQueryFor(max).mergeSorted(baseQueryFor(min))
      else if (max == min + 2) baseQueryFor(max).mergeSorted(baseQueryFor(min + 1).mergeSorted(baseQueryFor(min)))
      else {
        val size = 1 + max - min
        val midpoint = max - (size / 2)
        mergeTree(midpoint, max).mergeSorted(mergeTree(min, midpoint - 1))
      }
    }

    mergeTree(minJournal, maxJournal)
  }

  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    (EventsBySliceQueries, CurrentEventsBySliceQueries) match {
      case (Right(eventsBySliceQueries), Right(currentEventsBySliceQueries)) =>
        require(maxSlice >= minSlice, "maxSlice must be at least minSlice")
        requireTimestampOffset(offset)

        val hotJournal = journalRange.max
        val numSlices = 1 + maxSlice - minSlice
        Source
          .fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
            import GraphDSL.Implicits._

            val merge = builder.add(new Merge[EventEnvelope[Event]](numSlices, eagerComplete = false))
            (minSlice to maxSlice).iterator.zipWithIndex.foreach { case (slice, i) =>
              val sliceOffset = offsetForSlice(slice, offset)
              val hotSource = eventsBySliceQueries(hotJournal)
                .eventsBySlices[Event](entityType, slice, slice, sliceOffset)

              val source =
                if (journalRange.size == 1) hotSource
                else {
                  val coolerSource = (nextOffset: Option[TimestampOffset]) =>
                    unsafeCurrentEventsForSingleSlice[Event](
                      entityType,
                      slice,
                      nextOffset.getOrElse(sliceOffset),
                      journalRange.min,
                      hotJournal - 1,
                      currentEventsBySliceQueries)

                  Source.fromGraph(
                    new MergeHotWithCoolerStage(
                      hotSource,
                      coolerSource,
                      sliceOffset match {
                        case timestampOffset: TimestampOffset => Some(timestampOffset)
                        case NoOffset                         => None
                        case _                                => throw TimestampOffsetRequired
                      },
                      queryTimeWindow,
                      coldJournalBehindCurrentTime))
                }

              source ~> merge.in(i)
            }

            SourceShape(merge.out)
          })
          .map(_.removeMetadata[MultiJournalMetadata])

      case (Left(ex), _) => Source.failed(ex)
      case (_, Left(ex)) => Source.failed(ex)
    }

  private def offsetForSlice(slice: Int, offset: Offset): Offset =
    offset match {
      case TimestampOffsetBySlice(offsets) => offsets.getOrElse(slice, NoOffset)
      case other                           => other
    }

  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] =
    EventTimestampQueries match {
      case Right(queries) =>
        journalRange.foldLeft(Future.successful(Option.empty[Instant])) { (acc, journalId) =>
          val query = queries(journalId).timestampOf(persistenceId, sequenceNr)
          val flatMapped = acc.flatMap { prevResult =>
            query.map { newResult =>
              prevResult match {
                case None => newResult
                case Some(pr) =>
                  if (newResult.exists(_ != pr)) {
                    // it's potentially OK if an event was manually copied over from one journal to another
                    log.warn(
                      "Event for persistenceId [{}], seqNr [{}] was in multiple journals with different timestamps",
                      persistenceId,
                      sequenceNr)
                  }
                  prevResult
              }
            }(system.dispatcher)
          }(ExecutionContext.parasitic)

          flatMapped.recoverWith { case ex =>
            query
              .filter(_.isDefined)(ExecutionContext.parasitic)
              .recoverWith { case _: NoSuchElementException =>
                flatMapped // prefer the original exception
              }(ExecutionContext.parasitic)
          }(ExecutionContext.parasitic)
        }

      case Left(ex) => Future.failed(ex)
    }

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] =
    LoadEventQueries match {
      case Right(queries) =>
        journalRange.reverse.foldLeft(Future.failed[EventEnvelope[Event]](new NoSuchElementException)) {
          (acc, journalId) =>
            val query =
              queries(journalId)
                .loadEnvelope[Event](persistenceId, sequenceNr)

            acc
              .recoverWith { case nse: NoSuchElementException =>
                // not finding an event is somewhat expected, but pass other failures through
                query
              }(ExecutionContext.parasitic)
              .map(_.removeMetadata[MultiJournalMetadata])(ExecutionContext.parasitic)
        }

      case Left(ex) => Future.failed(ex)
    }

  private def allSupport[T](desc: String)(
      resolve: ReadJournalPair => Option[T]): Either[IllegalArgumentException, Map[Int, T]] = {
    val supported = readJournals.iterator.flatMap { case (id, journals) => resolve(journals).map(id -> _) }.toMap
    val unsupported = readJournals.keySet -- supported.keySet

    if (unsupported.isEmpty) Right(supported)
    else
      Left(
        new IllegalArgumentException(
          s"$desc is not supported by all read-journals: ${unsupported.iterator.mkString("[", ", ", "]")}")
          with NoStackTrace)
  }

  private def currentEventsByPersistenceIdQuery(
      journals: ReadJournalPair): Option[CurrentEventsByPersistenceIdTypedQuery] =
    journals.scala match {
      case query: CurrentEventsByPersistenceIdTypedQuery => Some(query)
      case _ =>
        journals.java match {
          case query: javaQuery.CurrentEventsByPersistenceIdTypedQuery =>
            Some(new CurrentEventsByPersistenceIdTypedQuery {
              override def currentEventsByPersistenceIdTyped[Event](
                  persistenceId: String,
                  fromSequenceNr: Long,
                  toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
                query.currentEventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr).asScala
            })
          case _ => None
        }
    }

  private def currentEventsBySliceQuery(journals: ReadJournalPair): Option[CurrentEventsBySliceQuery] =
    journals.scala match {
      case query: CurrentEventsBySliceQuery => Some(query)
      case _ =>
        journals.java match {
          case query: javaQuery.CurrentEventsBySliceQuery =>
            Some(new CurrentEventsBySliceQuery {
              override def currentEventsBySlices[Event](
                  entityType: String,
                  minSlice: Int,
                  maxSlice: Int,
                  offset: Offset): Source[EventEnvelope[Event], NotUsed] =
                query.currentEventsBySlices[Event](entityType, minSlice, maxSlice, offset).asScala

              override def sliceForPersistenceId(persistenceId: String): Int =
                query.sliceForPersistenceId(persistenceId)

              override def sliceRanges(numberOfRanges: Int): Seq[Range] =
                query.sliceRanges(numberOfRanges).asScala.map(pair => pair.first.intValue to pair.second.intValue).toSeq
            })
          case _ => None
        }
    }

  private def eventsBySliceQuery(journals: ReadJournalPair): Option[EventsBySliceQuery] =
    journals.scala match {
      case query: EventsBySliceQuery => Some(query)
      case _ =>
        journals.java match {
          case query: javaQuery.EventsBySliceQuery =>
            Some(new EventsBySliceQuery {
              override def eventsBySlices[Event](
                  entityType: String,
                  minSlice: Int,
                  maxSlice: Int,
                  offset: Offset): Source[EventEnvelope[Event], NotUsed] =
                query.eventsBySlices[Event](entityType, minSlice, maxSlice, offset).asScala

              override def sliceForPersistenceId(persistenceId: String): Int =
                query.sliceForPersistenceId(persistenceId)

              override def sliceRanges(numberOfRanges: Int): Seq[Range] =
                query.sliceRanges(numberOfRanges).asScala.map(pair => pair.first.intValue to pair.second.intValue).toSeq
            })
          case _ => None
        }
    }

  private def eventTimestampQuery(journals: ReadJournalPair): Option[EventTimestampQuery] =
    journals.scala match {
      case query: EventTimestampQuery => Some(query)
      case _ =>
        journals.java match {
          case query: javaQuery.EventTimestampQuery =>
            Some(new EventTimestampQuery {
              override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] =
                query.timestampOf(persistenceId, sequenceNr).asScala.map(_.toScala)(ExecutionContext.parasitic)
            })
          case _ => None
        }
    }

  private def loadEventQuery(journals: ReadJournalPair): Option[LoadEventQuery] =
    journals.scala match {
      case query: LoadEventQuery => Some(query)
      case _ =>
        journals.java match {
          case query: javaQuery.LoadEventQuery =>
            Some(new LoadEventQuery {
              override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] =
                query.loadEnvelope[Event](persistenceId, sequenceNr).asScala
            })
          case _ => None
        }
    }
}
