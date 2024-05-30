/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.internal

import java.time.Clock
import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.TimestampOffset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object DynamoDBOffsetStore {
  type SeqNr = Long
  type Pid = String

  final case class Record(slice: Int, pid: Pid, seqNr: SeqNr, timestamp: Instant)
  final case class RecordWithOffset(
      record: Record,
      offset: TimestampOffset,
      strictSeqNr: Boolean,
      fromBacktracking: Boolean,
      fromPubSub: Boolean,
      fromSnapshot: Boolean)
  final case class RecordWithProjectionKey(record: Record, projectionKey: String)

  object State {
    val empty: State = State(Map.empty, Vector.empty, Instant.EPOCH, 0)

    def apply(records: immutable.IndexedSeq[Record]): State = {
      if (records.isEmpty) empty
      else empty.add(records)
    }
  }

  final case class State(
      byPid: Map[Pid, Record],
      latest: immutable.IndexedSeq[Record],
      oldestTimestamp: Instant,
      sizeAfterEvict: Int) {

    def size: Int = byPid.size

    def latestTimestamp: Instant =
      if (latest.isEmpty) Instant.EPOCH
      else latest.head.timestamp

    def latestOffset: Option[TimestampOffset] = {
      if (latest.isEmpty)
        None
      else
        Some(TimestampOffset(latestTimestamp, latest.map(r => r.pid -> r.seqNr).toMap))
    }

    def add(records: immutable.IndexedSeq[Record]): State = {
      records.foldLeft(this) { case (acc, r) =>
        val newByPid =
          acc.byPid.get(r.pid) match {
            case Some(existingRecord) =>
              if (r.seqNr > existingRecord.seqNr)
                acc.byPid.updated(r.pid, r)
              else
                acc.byPid // older or same seqNr
            case None =>
              acc.byPid.updated(r.pid, r)
          }

        val latestTimestamp = acc.latestTimestamp
        val newLatest =
          if (r.timestamp.isAfter(latestTimestamp)) {
            Vector(r)
          } else if (r.timestamp == latestTimestamp) {
            acc.latest.find(_.pid == r.pid) match {
              case None                 => acc.latest :+ r
              case Some(existingRecord) =>
                // keep highest seqNr
                if (r.seqNr >= existingRecord.seqNr)
                  acc.latest.filterNot(_.pid == r.pid) :+ r
                else
                  acc.latest
            }
          } else {
            acc.latest // older than existing latest, keep existing latest
          }
        val newOldestTimestamp =
          if (acc.oldestTimestamp == Instant.EPOCH)
            r.timestamp // first record
          else if (r.timestamp.isBefore(acc.oldestTimestamp))
            r.timestamp
          else
            acc.oldestTimestamp // this is the normal case

        acc.copy(byPid = newByPid, latest = newLatest, oldestTimestamp = newOldestTimestamp)
      }
    }

    def isDuplicate(record: Record): Boolean = {
      byPid.get(record.pid) match {
        case Some(existingRecord) => record.seqNr <= existingRecord.seqNr
        case None                 => false
      }
    }

    def window: JDuration =
      JDuration.between(oldestTimestamp, latestTimestamp)

    private lazy val sortedByTimestamp: Vector[Record] = byPid.valuesIterator.toVector.sortBy(_.timestamp)

    lazy val latestBySlice: Vector[Record] = {
      val builder = scala.collection.mutable.Map[Int, Record]()
      sortedByTimestamp.reverseIterator.foreach { record =>
        if (!builder.contains(record.slice))
          builder.update(record.slice, record)
      }
      builder.values.toVector
    }

    def evict(until: Instant, keepNumberOfEntries: Int): State = {
      if (oldestTimestamp.isBefore(until) && size > keepNumberOfEntries) {
        val newState = State(
          sortedByTimestamp
            .take(size - keepNumberOfEntries)
            .filterNot(_.timestamp.isBefore(until)) ++ sortedByTimestamp
            .takeRight(keepNumberOfEntries) ++ latestBySlice)
        newState.copy(sizeAfterEvict = newState.size)
      } else
        this
    }

  }

  final class RejectedEnvelope(message: String) extends IllegalStateException(message)

  sealed trait Validation

  object Validation {
    case object Accepted extends Validation
    case object Duplicate extends Validation
    case object RejectedSeqNr extends Validation
    case object RejectedBacktrackingSeqNr extends Validation

    val FutureAccepted: Future[Validation] = Future.successful(Accepted)
    val FutureDuplicate: Future[Validation] = Future.successful(Duplicate)
    val FutureRejectedSeqNr: Future[Validation] = Future.successful(RejectedSeqNr)
    val FutureRejectedBacktrackingSeqNr: Future[Validation] = Future.successful(RejectedBacktrackingSeqNr)
  }

  val FutureDone: Future[Done] = Future.successful(Done)
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class DynamoDBOffsetStore(
    projectionId: ProjectionId,
    sourceProvider: Option[BySlicesSourceProvider],
    system: ActorSystem[_],
    settings: DynamoDBProjectionSettings,
    client: DynamoDbAsyncClient,
    clock: Clock = Clock.systemUTC()) {

  import DynamoDBOffsetStore._

  // FIXME include projectionId in all log messages
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val persistenceExt = Persistence(system)

  private val evictWindow = settings.timeWindow.plus(settings.evictInterval)

  private[projection] implicit val executionContext: ExecutionContext = system.executionContext

  // The OffsetStore instance is used by a single projectionId and there shouldn't be any concurrent
  // calls to methods that access the `state`. To detect any violations of that concurrency assumption
  // we use AtomicReference and fail if the CAS fails.
  private val state = new AtomicReference(State.empty)

  // Transient state of inflight pid -> seqNr (before they have been stored and included in `state`), which is
  // needed for at-least-once or other projections where the offset is saved afterwards. Not needed for exactly-once.
  // This can be updated concurrently with CAS retries.
  private val inflight = new AtomicReference(Map.empty[Pid, SeqNr])

  private def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    sourceProvider match {
      case Some(timestampQuery: EventTimestampQuery) =>
        timestampQuery.timestampOf(persistenceId, sequenceNr)
      case Some(timestampQuery: akka.persistence.query.typed.javadsl.EventTimestampQuery) =>
        import scala.jdk.FutureConverters._
        import scala.jdk.OptionConverters._
        timestampQuery.timestampOf(persistenceId, sequenceNr).asScala.map(_.toScala)
      case Some(_) =>
        throw new IllegalArgumentException(
          s"Expected BySlicesSourceProvider to implement EventTimestampQuery when TimestampOffset is used.")
      case None =>
        throw new IllegalArgumentException(
          s"Expected BySlicesSourceProvider to be defined when TimestampOffset is used.")
    }
  }

  def getState(): State =
    state.get()

  def getInflight(): Map[Pid, SeqNr] =
    inflight.get()

  def getOffset[Offset](): Future[Option[Offset]] = {
    getState().latestOffset match {
      case Some(t) => Future.successful(Some(t.asInstanceOf[Offset]))
      case None    => readOffset()
    }
  }

  def readOffset[Offset](): Future[Option[Offset]] = {
    // look for TimestampOffset first since that is used by akka-persistence-dynamodb,
    // and then fall back to the other more primitive offset types
    sourceProvider match {
      case Some(_) =>
        readTimestampOffset().map {
          case Some(t) => Some(t.asInstanceOf[Offset])
          case None    => None
        }
      case None =>
        // FIXME primitive offsets not supported, maybe we can change the sourceProvider parameter
        throw new IllegalStateException("BySlicesSourceProvider is required. Primitive offsets not supported.")
    }
  }

  private def readTimestampOffset(): Future[Option[TimestampOffset]] = {
    // FIXME
    clearInflight()
    ???
  }

  private def moreThanOneProjectionKey(recordsWithKey: immutable.IndexedSeq[RecordWithProjectionKey]): Boolean = {
    if (recordsWithKey.isEmpty)
      false
    else {
      val key = recordsWithKey.head.projectionKey
      recordsWithKey.exists(_.projectionKey != key)
    }
  }

  def saveOffset(offset: OffsetPidSeqNr): Future[Done] =
    saveOffsets(Vector(offset))

  def saveOffsets(offsets: immutable.IndexedSeq[OffsetPidSeqNr]): Future[Done] = {
    if (offsets.isEmpty)
      FutureDone
    else if (offsets.head.offset.isInstanceOf[TimestampOffset]) {
      val records = offsets.map {
        case OffsetPidSeqNr(t: TimestampOffset, Some((pid, seqNr))) =>
          val slice = persistenceExt.sliceForPersistenceId(pid)
          Record(slice, pid, seqNr, t.timestamp)
        case OffsetPidSeqNr(_: TimestampOffset, None) =>
          throw new IllegalArgumentException("Required EventEnvelope or DurableStateChange for TimestampOffset.")
        case _ =>
          throw new IllegalArgumentException(
            "Mix of TimestampOffset and other offset type in same transaction is not supported")
      }
      saveTimestampOffsets(records)
    } else {
      throw new IllegalStateException("TimestampOffset is required. Primitive offsets not supported.")
    }
  }

  private def saveTimestampOffsets(records: immutable.IndexedSeq[Record]): Future[Done] = {
    val oldState = state.get()
    val filteredRecords = {
      if (records.size <= 1)
        records.filterNot(oldState.isDuplicate)
      else {
        // use last record for each pid
        records
          .groupBy(_.pid)
          .valuesIterator
          .collect {
            case recordsByPid if !oldState.isDuplicate(recordsByPid.last) => recordsByPid.last
          }
          .toVector
      }
    }
    if (filteredRecords.isEmpty) {
      FutureDone
    } else {
      val newState = oldState.add(filteredRecords)

      // accumulate some more than the timeWindow before evicting, and at least 10% increase of size
      // for testing keepNumberOfEntries = 0 is used
      val evictThresholdReached =
        if (settings.keepNumberOfEntries == 0) true else newState.size > (newState.sizeAfterEvict * 1.1).toInt
      val evictedNewState =
        if (newState.size > settings.keepNumberOfEntries && evictThresholdReached && newState.window
            .compareTo(evictWindow) > 0) {
          val evictUntil = newState.latestTimestamp.minus(settings.timeWindow)
          val s = newState.evict(evictUntil, settings.keepNumberOfEntries)
          logger.debugN(
            "Evicted [{}] records until [{}], keeping [{}] records. Latest [{}].",
            newState.size - s.size,
            evictUntil,
            s.size,
            newState.latestTimestamp)
          s
        } else
          newState

      // FIXME
      val offsetInserts: Future[Done] = ???

      offsetInserts.map { _ =>
        if (state.compareAndSet(oldState, evictedNewState))
          cleanupInflight(evictedNewState)
        else
          throw new IllegalStateException("Unexpected concurrent modification of state from saveOffset.")
        Done
      }
    }
  }
  @tailrec private def cleanupInflight(newState: State): Unit = {
    val currentInflight = getInflight()
    val newInflight =
      currentInflight.filter { case (inflightPid, inflightSeqNr) =>
        newState.byPid.get(inflightPid) match {
          case Some(r) => r.seqNr < inflightSeqNr
          case None    => true
        }
      }
    if (newInflight.size >= 10000) {
      throw new IllegalStateException(
        s"Too many envelopes in-flight [${newInflight.size}]. " +
        "Please report this issue at https://github.com/akka/akka-persistence-dynamodb")
    }
    if (!inflight.compareAndSet(currentInflight, newInflight))
      cleanupInflight(newState) // CAS retry, concurrent update of inflight
  }

  @tailrec private def clearInflight(): Unit = {
    val currentInflight = getInflight()
    if (!inflight.compareAndSet(currentInflight, Map.empty[Pid, SeqNr]))
      clearInflight() // CAS retry, concurrent update of inflight
  }

  /**
   * The stored sequence number for a persistenceId, or 0 if unknown persistenceId.
   */
  def storedSeqNr(pid: Pid): SeqNr =
    getState().byPid.get(pid) match {
      case Some(record) => record.seqNr
      case None         => 0L
    }

  def validateAll[Envelope](envelopes: immutable.Seq[Envelope]): Future[immutable.Seq[(Envelope, Validation)]] = {
    import Validation._
    envelopes
      .foldLeft(Future.successful((getInflight(), Vector.empty[(Envelope, Validation)]))) { (acc, envelope) =>
        acc.flatMap { case (inflight, filteredEnvelopes) =>
          createRecordWithOffset(envelope) match {
            case Some(recordWithOffset) =>
              validate(recordWithOffset, inflight).map {
                case Accepted =>
                  (
                    inflight.updated(recordWithOffset.record.pid, recordWithOffset.record.seqNr),
                    filteredEnvelopes :+ (envelope -> Accepted))
                case rejected =>
                  (inflight, filteredEnvelopes :+ (envelope -> rejected))
              }
            case None =>
              Future.successful((inflight, filteredEnvelopes :+ (envelope -> Accepted)))
          }
        }
      }
      .map { case (_, filteredEnvelopes) =>
        filteredEnvelopes
      }
  }

  /**
   * Validate if the sequence number of the envelope is the next expected, or if the envelope is a duplicate that has
   * already been processed, or there is a gap in sequence numbers that should be rejected.
   */
  def validate[Envelope](envelope: Envelope): Future[Validation] = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) => validate(recordWithOffset, getInflight())
      case None                   => Validation.FutureAccepted
    }
  }

  private def validate(recordWithOffset: RecordWithOffset, currentInflight: Map[Pid, SeqNr]): Future[Validation] = {
    import Validation._
    val pid = recordWithOffset.record.pid
    val seqNr = recordWithOffset.record.seqNr
    val currentState = getState()

    val duplicate = getState().isDuplicate(recordWithOffset.record)

    if (duplicate) {
      logger.trace("Filtering out duplicate sequence number [{}] for pid [{}]", seqNr, pid)
      FutureDuplicate
    } else if (recordWithOffset.strictSeqNr) {
      // strictSeqNr == true is for event sourced
      val prevSeqNr = currentInflight.getOrElse(pid, currentState.byPid.get(pid).map(_.seqNr).getOrElse(0L))

      def logUnexpected(): Unit = {
        if (recordWithOffset.fromPubSub)
          logger.debugN(
            "Rejecting pub-sub envelope, unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
        else if (!recordWithOffset.fromBacktracking)
          logger.debugN(
            "Rejecting unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
        else
          logger.warnN(
            "Rejecting unexpected sequence number [{}] for pid [{}], previous sequence number [{}]. Offset: {}",
            seqNr,
            pid,
            prevSeqNr,
            recordWithOffset.offset)
      }

      def logUnknown(): Unit = {
        if (recordWithOffset.fromPubSub) {
          logger.debugN(
            "Rejecting pub-sub envelope, unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            seqNr,
            pid,
            recordWithOffset.offset)
        } else if (!recordWithOffset.fromBacktracking) {
          // This may happen rather frequently when using `publish-events`, after reconnecting and such.
          logger.debugN(
            "Rejecting unknown sequence number [{}] for pid [{}] (might be accepted later): {}",
            seqNr,
            pid,
            recordWithOffset.offset)
        } else {
          logger.warnN(
            "Rejecting unknown sequence number [{}] for pid [{}]. Offset: {}",
            seqNr,
            pid,
            recordWithOffset.offset)
        }
      }

      if (prevSeqNr > 0) {
        // expecting seqNr to be +1 of previously known
        val ok = seqNr == prevSeqNr + 1
        if (ok) {
          FutureAccepted
        } else if (seqNr <= currentInflight.getOrElse(pid, 0L)) {
          // currentInFlight contains those that have been processed or about to be processed in Flow,
          // but offset not saved yet => ok to handle as duplicate
          FutureDuplicate
        } else if (recordWithOffset.fromSnapshot) {
          // snapshots will mean we are starting from some arbitrary offset after last seen offset
          FutureAccepted
        } else if (!recordWithOffset.fromBacktracking) {
          logUnexpected()
          FutureRejectedSeqNr
        } else {
          logUnexpected()
          // This will result in projection restart (with normal configuration)
          FutureRejectedBacktrackingSeqNr
        }
      } else if (seqNr == 1) {
        // always accept first event if no other event for that pid has been seen
        FutureAccepted
      } else if (recordWithOffset.fromSnapshot) {
        // always accept starting from snapshots when there was no previous event seen
        FutureAccepted
      } else {
        // Haven't see seen this pid within the time window. Since events can be missed
        // when read at the tail we will only accept it if the event with previous seqNr has timestamp
        // before the time window of the offset store.
        // Backtracking will emit missed event again.
        timestampOf(pid, seqNr - 1).map {
          case Some(previousTimestamp) =>
            val before = currentState.latestTimestamp.minus(settings.timeWindow)
            if (previousTimestamp.isBefore(before)) {
              logger.debugN(
                "Accepting envelope with pid [{}], seqNr [{}], where previous event timestamp [{}] " +
                "is before time window [{}].",
                pid,
                seqNr,
                previousTimestamp,
                before)
              Accepted
            } else if (!recordWithOffset.fromBacktracking) {
              logUnknown()
              RejectedSeqNr
            } else {
              logUnknown()
              // This will result in projection restart (with normal configuration)
              RejectedBacktrackingSeqNr
            }
          case None =>
            // previous not found, could have been deleted
            Accepted
        }
      }
    } else {
      // strictSeqNr == false is for durable state where each revision might not be visible
      val prevSeqNr = currentInflight.getOrElse(pid, currentState.byPid.get(pid).map(_.seqNr).getOrElse(0L))
      val ok = seqNr > prevSeqNr

      if (ok) {
        FutureAccepted
      } else {
        logger.traceN("Filtering out earlier revision [{}] for pid [{}], previous revision [{}]", seqNr, pid, prevSeqNr)
        FutureDuplicate
      }
    }
  }

  @tailrec final def addInflight[Envelope](envelope: Envelope): Unit = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) =>
        val currentInflight = getInflight()
        val newInflight = currentInflight.updated(recordWithOffset.record.pid, recordWithOffset.record.seqNr)
        if (!inflight.compareAndSet(currentInflight, newInflight))
          addInflight(envelope) // CAS retry, concurrent update of inflight
      case None =>
    }
  }

  @tailrec final def addInflights[Envelope](envelopes: immutable.Seq[Envelope]): Unit = {
    val currentInflight = getInflight()
    val entries = envelopes.iterator.map(createRecordWithOffset).collect { case Some(r) =>
      r.record.pid -> r.record.seqNr
    }
    val newInflight = currentInflight ++ entries
    if (!inflight.compareAndSet(currentInflight, newInflight))
      addInflights(envelopes) // CAS retry, concurrent update of inflight
  }

  def isInflight[Envelope](envelope: Envelope): Boolean = {
    createRecordWithOffset(envelope) match {
      case Some(recordWithOffset) =>
        val pid = recordWithOffset.record.pid
        val seqNr = recordWithOffset.record.seqNr
        getInflight().get(pid) match {
          case Some(`seqNr`) => true
          case _             => false
        }
      case None => true
    }
  }

  private def createRecordWithOffset[Envelope](envelope: Envelope): Option[RecordWithOffset] = {
    envelope match {
      case eventEnvelope: EventEnvelope[_] if eventEnvelope.offset.isInstanceOf[TimestampOffset] =>
        val timestampOffset = eventEnvelope.offset.asInstanceOf[TimestampOffset]
        val slice = persistenceExt.sliceForPersistenceId(eventEnvelope.persistenceId)
        Some(
          RecordWithOffset(
            Record(slice, eventEnvelope.persistenceId, eventEnvelope.sequenceNr, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr = true,
            fromBacktracking = EnvelopeOrigin.fromBacktracking(eventEnvelope),
            fromPubSub = EnvelopeOrigin.fromPubSub(eventEnvelope),
            fromSnapshot = EnvelopeOrigin.fromSnapshot(eventEnvelope)))
      case change: UpdatedDurableState[_] if change.offset.isInstanceOf[TimestampOffset] =>
        val timestampOffset = change.offset.asInstanceOf[TimestampOffset]
        val slice = persistenceExt.sliceForPersistenceId(change.persistenceId)
        Some(
          RecordWithOffset(
            Record(slice, change.persistenceId, change.revision, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr = false,
            fromBacktracking = EnvelopeOrigin.fromBacktracking(change),
            fromPubSub = false,
            fromSnapshot = false))
      case change: DeletedDurableState[_] if change.offset.isInstanceOf[TimestampOffset] =>
        val timestampOffset = change.offset.asInstanceOf[TimestampOffset]
        val slice = persistenceExt.sliceForPersistenceId(change.persistenceId)
        Some(
          RecordWithOffset(
            Record(slice, change.persistenceId, change.revision, timestampOffset.timestamp),
            timestampOffset,
            strictSeqNr = false,
            fromBacktracking = false,
            fromPubSub = false,
            fromSnapshot = false))
      case change: DurableStateChange[_] if change.offset.isInstanceOf[TimestampOffset] =>
        // in case additional types are added
        throw new IllegalArgumentException(
          s"DurableStateChange [${change.getClass.getName}] not implemented yet. Please report bug at https://github.com/akka/akka-projection/issues")
      case _ => None
    }
  }

}
