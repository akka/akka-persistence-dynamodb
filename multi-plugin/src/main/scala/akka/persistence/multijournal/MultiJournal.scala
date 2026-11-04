/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.jdk.DurationConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NoStackTrace
import scala.util.control.NonFatal

import com.typesafe.config.Config

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.CoordinatedShutdown
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.StashFactory
import akka.annotation.InternalApi
import akka.dispatch.DequeBasedMessageQueueSemantics
import akka.dispatch.Envelope
import akka.dispatch.RequiresMessageQueue
import akka.event.Logging
import akka.persistence.AtomicWrite
import akka.persistence.CompositeMetadata
import akka.persistence.DeleteMessagesFailure
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.JournalProtocol
import akka.persistence.PersistentImpl
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncReplay
import akka.persistence.journal.AsyncWriteJournal

@InternalApi
private[multijournal] object MultiJournal {
  // At this point, this plugin only supports being used with DynamoDB, as well as a test implementation
  val AllowedJournals =
    Set("akka.persistence.dynamodb.journal.DynamoDBJournal", "akka.persistence.multijournal.TestProbeJournal")

  private final class PerPidHandle(val ref: ActorRef, var lastAccess: Instant)

  def perPidHandle(pid: String, idleTimeout: FiniteDuration, journals: Map[Int, ActorRef]): Props =
    Props(new PerPidActor(pid, idleTimeout, journals))

  private final class PerPidActor(val pid: String, idleTimeout: FiniteDuration, val journals: Map[Int, ActorRef])
      extends Actor
      with ActorLogging
      with RequiresMessageQueue[DequeBasedMessageQueueSemantics]
      with StashFactory {
    // assumed that journals is non-empty, gapless, and all actors in it are actual tell-able actors
    // spawners of this actor need to validate that
    val (hotJournalId, hotJournal) = journals.maxBy(_._1)
    val (minJournalId, _) = journals.minBy(_._1)

    context.setReceiveTimeout(idleTimeout)

    override def postStop(): Unit = {
      if (pendingPromises.nonEmpty) {
        pendingPromises.foreach(_.tryFailure(PerPidActorStopping))
      }

      super.postStop()
    }

    val generalBehavior: Receive = {
      case GetMetadata(`pid`, promise) =>
        // NB: does not attempt to reconcile or retrieve metadata: solely for introspection in tests
        if (currentAction.isEmpty) {
          promise.trySuccess(metadata.fold(Map.empty[Int, Long])(_.toMap))
        } else {
          waitingForMetadata = waitingForMetadata.incl(promise)
        }

      case ReceiveTimeout =>
        val idleSince = Instant.now().minus(javaIdleTimeout)
        log.debug("Per-pid actor for [{}] idle since [{}]", pid, idleSince)
        context.parent ! PerPidActorIdleSince(pid, idleSince, self)

      case cmd: PerPidCommand if draining =>
        // our parent will hand this to the next per-pid actor
        val promise = cmd.promiseForFailure
        pendingPromises = pendingPromises.excl(promise)
        if (!promise.isCompleted) context.parent ! DeferredByDrain(pid, cmd)

      case r @ ReplayMessages(fromSequenceNr, toSequenceNr, max, _, maxSeqNrPromise) =>
        pendingPromises = pendingPromises.incl(maxSeqNrPromise)
        if (currentAction.isEmpty) {
          // we always recover from the hot journal first, since we need to get the max-seqNr and the latest metadata
          hotJournal ! JournalProtocol.ReplayMessages(fromSequenceNr, toSequenceNr, max, pid, self)
          transitionTo(
            Some(
              if (hotJournalId == minJournalId) new ReplayingFromOnly(r)
              else new ReplayingFromHot(r)))
        } else {
          pendingReplay.foreach { pending =>
            // later replay supersedes current replay
            fail(pending.maxSeqNrPromise, OperationSuperseded)
          }

          pendingReplay = Some(r)
        }

      case d @ DeleteMessagesTo(toSequenceNr, donePromise) =>
        val undoPending = pendingPromises
        pendingPromises = pendingPromises.incl(donePromise)
        if (currentAction.isEmpty) startWrite(d)
        else {
          pendingWrite match {
            case None => pendingWrite = Some(d)
            case Some(_: WriteMessages) =>
              donePromise.tryFailure(OperationSuperseded)
              pendingPromises = undoPending

            case Some(DeleteMessagesTo(target, otherDonePromise)) if target >= toSequenceNr =>
              donePromise.completeWith(otherDonePromise.future)
              pendingPromises = undoPending

            case Some(DeleteMessagesTo(_, otherDonePromise)) =>
              otherDonePromise.completeWith(donePromise.future)
              pendingPromises = pendingPromises.excl(otherDonePromise)
              pendingWrite = Some(d)
          }
        }

      case w @ WriteMessages(_, promise) =>
        pendingPromises = pendingPromises.incl(promise)
        if (currentAction.isEmpty) startWrite(w)
        else {
          pendingWrite.foreach { pending =>
            fail(pending.promiseForFailure, OperationSuperseded)
          }

          pendingWrite = Some(w)
        }

      case ootb =>
        log.info(
          "Out-of-the-blue message for per-pid actor [{}]: message class={}, sender={}",
          pid,
          ootb.getClass.getName,
          sender())
    }

    def receive: Receive = generalBehavior

    def transitionTo(next: Option[CurrentAction]): Unit = {
      val nextBehavior = next match {
        case None =>
          if (waitingForMetadata.nonEmpty) {
            val metaMap = metadata.fold(Map.empty[Int, Long])(_.toMap)
            waitingForMetadata.foreach(_.trySuccess(metaMap))
          }

          if (pendingReplay.orElse[Any](pendingWrite).nonEmpty) {
            val stash = createStash()
            stash.prepend(
              Iterator(pendingReplay, pendingWrite).flatten
                .map(m => Envelope(m, ActorRef.noSender, context.system))
                .toSeq)
            stash.unstashAll()
          }
          pendingReplay = None
          pendingWrite = None
          PartialFunction.empty

        case Some(action) =>
          action.behavior(this)
      }
      currentAction = next
      context.become(nextBehavior.orElse(generalBehavior))
    }

    // these vars may be accessed by the current-action behaviors
    /** Promises we have received but not completed... must fail them if the actor stops */
    var pendingPromises: Set[Promise[_]] = Set.empty
    var metadata: Option[MultiJournalMetadata] = None
    var waitingForMetadata: Set[Promise[Map[Int, Long]]] = Set.empty
    val instanceId: Int = ThreadLocalRandom.current().nextInt()
    var draining: Boolean = false

    def drainThenStop(): Unit = {
      // once our parent gets this, it will stop sending us messages (potentially starting a new per-pid actor)
      context.parent ! PerPidActorRequestsStop(pid, self)
      draining = true
      transitionTo(None)
    }

    private var currentAction: Option[CurrentAction] = None
    private val javaIdleTimeout = idleTimeout.toJava
    private var pendingReplay: Option[ReplayMessages] = None
    private var pendingWrite: Option[WriteOp] = None

    def admissibleMetaMap(m: MultiJournalMetadata, maxAllowed: Int): Map[Int, Long] =
      m.toMap.filter { case (id, _) => id < maxAllowed }

    /** As admissibleMetaMap, but avoids allocating when there's nothing to remove */
    def admissibleMetadata(m: MultiJournalMetadata, maxAllowed: Int): MultiJournalMetadata =
      if (m.bounds._2 < maxAllowed) m else MultiJournalMetadata(admissibleMetaMap(m, maxAllowed))

    def succeed[T](promise: Promise[T], value: T): Unit = {
      promise.trySuccess(value)
      pendingPromises = pendingPromises.excl(promise)
    }

    def fail(promise: Promise[_], ex: Throwable): Unit = {
      promise.tryFailure(ex)
      pendingPromises = pendingPromises.excl(promise)
    }

    def failAndTransition(promise: Promise[_], ex: Throwable): Unit = {
      fail(promise, ex)
      metadata = None
      transitionTo(None)
    }

    def failAndDrain(promise: Promise[_], ex: Throwable): Unit = {
      fail(promise, ex)
      drainThenStop()
    }

    def startWrite(cmd: WriteOp): Unit =
      if (metadata.isEmpty) {
        hotJournal ! JournalProtocol.ReplayMessages(-1, Long.MaxValue, 1, pid, self)
        transitionTo(Some(new RecoveringMetadataForWrite(cmd, minJournalId to hotJournalId)))
      } else transitionTo(nextActionForWrite(cmd))

    def nextActionForWrite(cmd: WriteOp): Option[CurrentAction] =
      cmd match {
        case d @ DeleteMessagesTo(toSequenceNr, _) =>
          metadata match {
            case Some(m) =>
              m.locateSequenceNr(toSequenceNr) match {
                case Some(id) if id >= minJournalId =>
                  Some(new DeletingEvents(d, minJournalId to id))

                case None if m.maxSeqNrKnown.exists(_ < toSequenceNr) =>
                  Some(new DeletingEvents(d, minJournalId to hotJournalId))

                case None if m.isEmpty && toSequenceNr > 0 =>
                  Some(new DeletingEvents(d, hotJournalId to hotJournalId))

                case _ =>
                  // no-op: we don't have any events up to toSequenceNr
                  succeed(d.promise, ())
                  None
              }

            case None =>
              Some(new DeletingEvents(d, minJournalId to hotJournalId))
          }

        case w: WriteMessages =>
          Some(new WritingEvents(w))
      }
  }

  private def stripMultiJournalMetadata(repr: PersistentRepr): (PersistentRepr, Option[MultiJournalMetadata]) = {
    def withoutMetadata: PersistentRepr =
      PersistentImpl(
        payload = repr.payload,
        sequenceNr = repr.sequenceNr,
        persistenceId = repr.persistenceId,
        manifest = repr.manifest,
        deleted = repr.deleted,
        sender = repr.sender,
        writerUuid = repr.writerUuid,
        timestamp = repr.timestamp,
        metadata = None)

    repr.metadata match {
      case None => repr -> None
      case current @ Some(meta) =>
        meta match {
          case multi: MultiJournalMetadata => withoutMetadata -> Some(multi)
          case composite: CompositeMetadata =>
            CompositeMetadata.extract[MultiJournalMetadata](current) match {
              case m @ Some(meta) =>
                CompositeMetadata.construct(composite.entries.filterNot(_ == meta)) match {
                  case None               => withoutMetadata -> m
                  case Some(strippedMeta) => repr.withMetadata(strippedMeta) -> m
                }

              case None => repr -> None
            }
          case _ => repr -> None
        }
    }
  }

  private[multijournal] final case class GetMetadata(pid: String, promise: Promise[Map[Int, Long]])

  private final case class PerPidActorIdleSince(pid: String, time: Instant, actor: ActorRef)

  private sealed trait PerPidCommand {
    def promiseForFailure: Promise[Nothing]
  }

  private final case class ReplayMessages(
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long,
      callback: PersistentRepr => Unit,
      maxSeqNrPromise: Promise[Long])
      extends PerPidCommand {
    def isMaxSeqNrQuery: Boolean = toSequenceNr == 0
    def isReplayOnlyLast: Boolean = fromSequenceNr == -1
    def promiseForFailure: Promise[Nothing] = maxSeqNrPromise.asInstanceOf[Promise[Nothing]]
  }

  private final case class ReplayLeg(journalId: Int, fromSequenceNr: Long, toSequenceNr: Long)

  private def metadataDirectedReplay(
      cmd: ReplayMessages,
      metadata: MultiJournalMetadata,
      journalIds: IndexedSeq[Int],
      effectiveToSequenceNr: Long): Option[Vector[ReplayLeg]] =
    if (cmd.isMaxSeqNrQuery || cmd.isReplayOnlyLast) None
    else {
      val legs = Vector.newBuilder[ReplayLeg]
      var complete = true

      journalIds.foreach { journalId =>
        metadata.get(journalId) match {
          case None => complete = false
          case Some(upperBound) if cmd.fromSequenceNr <= upperBound =>
            val lowerBound =
              // Metadata for journals colder than the configured coldest journal is deliberately stripped.
              if (journalId == journalIds.head) Some(0L)
              else metadata.get(journalId - 1)

            lowerBound match {
              case Some(lower) if lower < Long.MaxValue =>
                val from = math.max(cmd.fromSequenceNr, lower + 1)
                val to = math.min(effectiveToSequenceNr, upperBound)
                if (from <= to) legs += ReplayLeg(journalId, from, to)

              case Some(_) => // no sequence number can be greater than this lower bound
              case None    => complete = false
            }

          case Some(_) => // the requested interval starts above this journal's upper bound
        }
      }

      if (complete) Some(legs.result()) else None
    }

  private sealed trait WriteOp extends PerPidCommand

  private final case class DeleteMessagesTo(toSequenceNr: Long, promise: Promise[Unit]) extends WriteOp {
    def promiseForFailure: Promise[Nothing] = promise.asInstanceOf[Promise[Nothing]]
  }

  private final case class WriteMessages(messages: Seq[AtomicWrite], promise: Promise[Seq[Try[Unit]]]) extends WriteOp {
    def promiseForFailure: Promise[Nothing] = promise.asInstanceOf[Promise[Nothing]]
  }

  private sealed trait CurrentAction {

    /** Only safe to call within the actor thread */
    def behavior(actor: PerPidActor): Actor.Receive
  }

  private def probeMetaOrTransitionTo(
      actor: PerPidActor)(highestSeqNr: Long, journal: ActorRef, nextActionThunk: () => Option[CurrentAction])(
      recordProbe: Long => Unit): Unit = {
    actor.metadata match {
      case None =>
        journal ! JournalProtocol.ReplayMessages(-1, Long.MaxValue, 1, actor.pid, actor.self)
        recordProbe(highestSeqNr)

      case _ => actor.transitionTo(nextActionThunk())
    }
  }

  private final class ReplayingFromOnly(_cmd: ReplayMessages) extends CurrentAction {
    // No cooler journals
    def behavior(actor: PerPidActor): Actor.Receive = {
      var reversedMessages: List[PersistentRepr] = Nil
      // as found on the latest replayed event
      var latestMetadata: Option[MultiJournalMetadata] = None
      var probingSeqNr: Option[Long] = None
      val pid = actor.pid
      val log = actor.log

      def admissibleLatestMetadata: Option[MultiJournalMetadata] =
        latestMetadata.map(actor.admissibleMetadata(_, actor.hotJournalId))

      val probeMetaOrTransitionToNone: Long => Unit = probeMetaOrTransitionTo(actor)(_, actor.hotJournal, ThunkNone) {
        highestSeqNr =>
          probingSeqNr = Some(highestSeqNr)
      }

      {
        case JournalProtocol.ReplayMessagesFailure(cause) =>
          actor.failAndTransition(promise, cause)

        case JournalProtocol.RecoverySuccess(highSeqNr) if probingSeqNr.nonEmpty =>
          if (probingSeqNr.contains(highSeqNr)) {
            actor.metadata = admissibleLatestMetadata.orElse(SomeEmptyMetadata)
            actor.transitionTo(None)
          } else {
            log.error(
              "Detected competing write for [{}], stopping per-pid actor (expected={}, actual={})",
              pid,
              probingSeqNr.get,
              highSeqNr)
            actor.failAndDrain(promise, PerPidActorStopping)
          }

        case JournalProtocol.RecoverySuccess(0) if reversedMessages.nonEmpty =>
          log.error(
            "JournalProtocol violation: Per-pid actor for [{}] received messages from the journal but highestSeqNr was 0",
            pid)
          actor.failAndTransition(promise, JournalProtocolViolation)

        case JournalProtocol.RecoverySuccess(0) =>
          actor.metadata = SomeEmptyMetadata
          actor.succeed(promise, 0L)
          actor.transitionTo(None)

        case JournalProtocol.RecoverySuccess(highestSeqNr) if _cmd.isReplayOnlyLast =>
          if (reversedMessages.drop(1).nonEmpty) {
            log.error(
              "JournalProtocol violation: Per-pid actor for [{}] received more messages from the journal than requested",
              pid)
            actor.failAndTransition(promise, JournalProtocolViolation)
          } else {
            reversedMessages.foreach(_cmd.callback)
            actor.succeed(promise, highestSeqNr)
            actor.metadata = admissibleLatestMetadata
            actor.transitionTo(None)
          }

        case JournalProtocol.RecoverySuccess(highestSeqNr) if _cmd.isMaxSeqNrQuery =>
          if (reversedMessages.nonEmpty) {
            log.error(
              "JournalProtocol violation: Per-pid actor for [{}] received messages when only requesting highestSeqNr",
              pid)
            actor.failAndTransition(promise, JournalProtocolViolation)
          } else {
            actor.succeed(promise, highestSeqNr)
            probeMetaOrTransitionToNone(highestSeqNr)
          }

        case JournalProtocol.RecoverySuccess(highestSeqNr) =>
          if (_cmd.fromSequenceNr > highestSeqNr) {
            actor.succeed(promise, highestSeqNr)
            probeMetaOrTransitionToNone(highestSeqNr)
          } else {
            reversedMessages.headOption match {
              case None =>
                actor.succeed(promise, highestSeqNr)

                if (_cmd.toSequenceNr >= highestSeqNr) {
                  actor.metadata = SomeEmptyMetadata
                  actor.transitionTo(None)
                } else {
                  probeMetaOrTransitionToNone(highestSeqNr)
                }

              case Some(repr) =>
                reversedMessages.reverse.foreach(_cmd.callback)
                actor.succeed(promise, highestSeqNr)
                if (repr.sequenceNr == highestSeqNr) {
                  actor.metadata = admissibleLatestMetadata.orElse(SomeEmptyMetadata)
                  actor.transitionTo(None)
                } else probeMetaOrTransitionToNone(highestSeqNr)
            }
          }

        case JournalProtocol.ReplayedMessage(repr) =>
          // when probing, only the probed event matters
          if (probingSeqNr.forall(_ == repr.sequenceNr)) {
            val (stripped, meta) = stripMultiJournalMetadata(repr)
            // missing metadata is OK, since this is (vacuously) the coldest journal as well as the hottest
            latestMetadata = meta
            if (probingSeqNr.isEmpty) reversedMessages = stripped :: reversedMessages
          }
      }
    }

    private def promise = _cmd.maxSeqNrPromise
  }

  private final class ReplayingFromHot(_cmd: ReplayMessages) extends CurrentAction {
    def behavior(actor: PerPidActor): Actor.Receive = {
      var reversedMessages: List[PersistentRepr] = Nil
      // as found on the latest replayed event
      var latestMetadata: Option[MultiJournalMetadata] = None
      var probing: Option[(Long, () => Option[CurrentAction])] = None
      val pid = actor.pid
      val log = actor.log

      def admissibleLatestMetadata: Option[MultiJournalMetadata] =
        latestMetadata.map(actor.admissibleMetadata(_, actor.hotJournalId))

      val probeMetaAndThen: (Long, () => Option[CurrentAction]) => Unit = (highestSeqNr, nextActionThunk) =>
        probeMetaOrTransitionTo(actor)(highestSeqNr, actor.hotJournal, nextActionThunk) { h =>
          probing = Some(h -> nextActionThunk)
        }

      val probeMetaAndComplete: Long => Unit = probeMetaAndThen(_, ThunkNone)

      {
        case JournalProtocol.ReplayMessagesFailure(cause) =>
          actor.failAndTransition(promise, cause)

        case JournalProtocol.RecoverySuccess(highSeqNr) if probing.nonEmpty =>
          val (probeSeqNr, next) = probing.get
          if (probeSeqNr == highSeqNr) {
            actor.metadata = admissibleLatestMetadata
            actor.transitionTo(next())
          } else {
            log.error(
              "Detected competing write for [{}], stopping per-pid actor (expected={}, actual={})",
              pid,
              probeSeqNr,
              highSeqNr)
            actor.failAndDrain(promise, PerPidActorStopping)
          }

        case JournalProtocol.RecoverySuccess(0) if reversedMessages.nonEmpty =>
          log.error(
            "JournalProtocol violation: Per-pid actor for [{}] received messages from the journal but highestSeqNr was 0",
            pid)
          actor.failAndTransition(promise, JournalProtocolViolation)

        case JournalProtocol.RecoverySuccess(0) =>
          actor.transitionTo(transitionToCooler(actor, Nil, 0)())

        case JournalProtocol.RecoverySuccess(highestSeqNr) if _cmd.isReplayOnlyLast =>
          if (reversedMessages.drop(1).nonEmpty) {
            log.error(
              "JournalProtocol violation: Per-pid actor for [{}] received more messages from the journal than requested",
              pid)
            actor.failAndTransition(promise, JournalProtocolViolation)
          } else {
            reversedMessages.foreach(_cmd.callback)
            actor.succeed(promise, highestSeqNr)
            actor.metadata = admissibleLatestMetadata
            probeMetaAndComplete(highestSeqNr)
          }

        case JournalProtocol.RecoverySuccess(highestSeqNr) if _cmd.isMaxSeqNrQuery =>
          if (reversedMessages.nonEmpty) {
            log.error(
              "JournalProtocol violation: Per-pid actor for [{}] received messages when only requesting highestSeqNr",
              pid)
            actor.failAndTransition(promise, JournalProtocolViolation)
          } else {
            actor.succeed(promise, highestSeqNr)
            probeMetaAndComplete(highestSeqNr)
          }

        case JournalProtocol.RecoverySuccess(highestSeqNr) =>
          if (_cmd.fromSequenceNr > highestSeqNr) {
            actor.succeed(promise, highestSeqNr)
            probeMetaAndComplete(highestSeqNr)
          } else {
            reversedMessages.headOption match { // last msg received from journal
              case None =>
                if (_cmd.toSequenceNr >= highestSeqNr) {
                  // if there's actually an event with the highestSeqNr, it would be reversedMessages.head
                  // we can assume that this means all messages up to and including highestSeqNr were deleted/expired, so no point in
                  // going to cooler journals or probing
                  actor.succeed(promise, highestSeqNr)
                  actor.metadata = SomeEmptyMetadata
                  actor.transitionTo(None)
                } else {
                  // probe meta then transition to next coolest
                  probeMetaAndThen(highestSeqNr, transitionToCooler(actor, Nil, highestSeqNr))
                }

              case Some(lastRepr) =>
                val inOrder = reversedMessages.reverse
                if (inOrder.head.sequenceNr == _cmd.fromSequenceNr) {
                  inOrder.foreach(_cmd.callback)
                  actor.succeed(promise, highestSeqNr)

                  if (lastRepr.sequenceNr == highestSeqNr) {
                    actor.metadata = admissibleLatestMetadata
                    actor.transitionTo(None)
                  } else probeMetaAndComplete(highestSeqNr)
                } else {
                  if (lastRepr.sequenceNr == highestSeqNr) {
                    actor.metadata = admissibleLatestMetadata
                    // transition to next coolest
                    actor.transitionTo(transitionToCooler(actor, inOrder, highestSeqNr)())
                  } else {
                    // probe meta and then transition to next coolest
                    probeMetaAndThen(highestSeqNr, transitionToCooler(actor, inOrder, highestSeqNr))
                  }
                }
            }
          }

        case JournalProtocol.ReplayedMessage(repr) =>
          // when probing, only the probed event matters
          if (probing.forall(_._1 == repr.sequenceNr)) {
            val (stripped, meta) = stripMultiJournalMetadata(repr)
            if (meta.isEmpty) {
              log.error(
                "Detected persisted event missing metadata in hot journal for [{}], at seqNr [{}].  Stopping per-pid actor",
                pid,
                repr.sequenceNr)
              actor.failAndDrain(promise, MissingExpectedMetadata)
            } else {
              latestMetadata = meta
              if (probing.isEmpty) reversedMessages = stripped :: reversedMessages
            }
          }
      }
    }

    def transitionToCooler(actor: PerPidActor, inOrder: Seq[PersistentRepr], highSeqNr: Long) = () => {
      val foundMetadata = actor.metadata.exists(!_.isEmpty)
      val journalRange = actor.minJournalId until actor.hotJournalId
      val highSeqs = if (highSeqNr > 0) SortedMap(actor.hotJournalId -> highSeqNr) else SortedMap.empty[Int, Long]
      val effectiveTo = inOrder.headOption.fold(_cmd.toSequenceNr)(_.sequenceNr - 1)
      val directedReplay = actor.metadata.filterNot(_.isEmpty).flatMap { metadata =>
        metadataDirectedReplay(_cmd, metadata, journalRange, effectiveTo)
      }

      directedReplay match {
        case Some(Vector()) =>
          inOrder.foreach(_cmd.callback)
          actor.succeed(promise, highSeqNr)
          None

        case Some(legs) =>
          val leg = legs.last
          actor.journals(leg.journalId) ! JournalProtocol.ReplayMessages(
            leg.fromSequenceNr,
            leg.toSequenceNr,
            _cmd.max,
            actor.pid,
            actor.self)
          Some(new ReplayingFromCool(_cmd, journalRange, Some(legs), Vector(inOrder), highSeqs, foundMetadata))

        case None =>
          val journalCmd =
            if (_cmd.isMaxSeqNrQuery || _cmd.isReplayOnlyLast)
              JournalProtocol.ReplayMessages(_cmd.fromSequenceNr, _cmd.toSequenceNr, _cmd.max, actor.pid, actor.self)
            else JournalProtocol.ReplayMessages(_cmd.fromSequenceNr, effectiveTo, _cmd.max, actor.pid, actor.self)

          actor.journals(journalRange.last) ! journalCmd
          Some(new ReplayingFromCool(_cmd, journalRange, None, Vector(inOrder), highSeqs, foundMetadata))
      }
    }

    private def promise = _cmd.maxSeqNrPromise
  }

  private final class ReplayingFromCool(
      _cmd: ReplayMessages,
      journals: Range,
      directedReplay: Option[Vector[ReplayLeg]],
      replayedInOrder: Vector[Seq[PersistentRepr]],
      highSeqNrs: SortedMap[Int, Long],
      foundMetadata: Boolean)
      extends CurrentAction {
    def behavior(actor: PerPidActor): Actor.Receive = {
      var reversedMessages: List[PersistentRepr] = Nil
      // as found on the latest replayed event
      var latestMetadata: Option[MultiJournalMetadata] = None
      var probing: Option[(Long, () => Option[CurrentAction])] = None
      val pid = actor.pid
      val log = actor.log
      val isColdestJournal = currentJournal == actor.minJournalId

      def admissibleLatestMetadata: Option[MultiJournalMetadata] =
        latestMetadata.map { m =>
          MultiJournalMetadata(actor.admissibleMetaMap(m, currentJournal) ++ highSeqNrs.removed(actor.hotJournalId))
        }

      val probeMetaAndThen: (Long, () => Option[CurrentAction]) => Unit = (highestSeqNr, nextActionThunk) =>
        probeMetaOrTransitionTo(actor)(highestSeqNr, actor.journals(currentJournal), nextActionThunk) { h =>
          probing = Some(h -> nextActionThunk)
        }

      val probeMetaOrTransitionToNone: Long => Unit = probeMetaAndThen(_, ThunkNone)

      {
        case JournalProtocol.ReplayMessagesFailure(cause) =>
          actor.failAndTransition(promise, cause)

        case JournalProtocol.RecoverySuccess(highSeqNr) if probing.nonEmpty =>
          val (probeSeqNr, next) = probing.get
          if (probeSeqNr == highSeqNr) {
            actor.metadata =
              admissibleLatestMetadata.orElse(SomeEmptyMetadata).map(_.updated(currentJournal, highSeqNr))
            actor.transitionTo(next())
          } else {
            log.error(
              "Detected competing write for [{}], stopping per-pid actor (expected={}, actual={})",
              pid,
              probeSeqNr,
              highSeqNr)
            actor.failAndDrain(promise, PerPidActorStopping)
          }

        case JournalProtocol.RecoverySuccess(0) if reversedMessages.nonEmpty =>
          log.error(
            "JournalProtocol violation: Per-pid actor for [{}] received messages from the journal but highestSeqNr was 0",
            pid)
          actor.failAndTransition(promise, JournalProtocolViolation)

        case JournalProtocol.RecoverySuccess(0) =>
          if (isColdestJournal) {
            if (!foundMetadata) {
              actor.metadata = Some(MultiJournalMetadata(highSeqNrs.removed(actor.hotJournalId)))
            }
            replayedInOrder.iterator.flatten.foreach(_cmd.callback)
            val highestSeqNr = if (highSeqNrs.isEmpty) 0L else highSeqNrs.last._2
            actor.succeed(promise, highestSeqNr)
            actor.transitionTo(None)
          } else {
            actor.transitionTo(transitionToCooler(actor, Nil, 0)())
          }

        case JournalProtocol.RecoverySuccess(highestSeqNr) if _cmd.isReplayOnlyLast =>
          if (reversedMessages.drop(1).nonEmpty) {
            log.error(
              "JournalProtocol violation: Per-pid actor for [{}] received more messages from the journal than requested",
              pid)
            actor.failAndTransition(promise, JournalProtocolViolation)
          } else {
            reversedMessages.foreach(_cmd.callback)
            actor.succeed(promise, highestSeqNr)
            if (!foundMetadata) {
              actor.metadata =
                admissibleLatestMetadata.orElse(SomeEmptyMetadata).map(_.updated(currentJournal, highestSeqNr))
            }
            actor.transitionTo(None)
          }

        case JournalProtocol.RecoverySuccess(highestSeqNr) if _cmd.isMaxSeqNrQuery =>
          if (reversedMessages.nonEmpty) {
            log.error(
              "JournalProtocol violation: Per-pid actor for [{}] received messages when only requesting highestSeqNr",
              pid)
            actor.failAndTransition(promise, JournalProtocolViolation)
          } else {
            actor.succeed(promise, highestSeqNr)
            if (!foundMetadata) {
              probeMetaOrTransitionToNone(highestSeqNr)
            } else actor.transitionTo(None)
          }

        case JournalProtocol.RecoverySuccess(highestSeqNr) =>
          val maxSeqNr = highSeqNrs.lastOption.fold(highestSeqNr)(_._2)
          if (_cmd.fromSequenceNr > highestSeqNr) {
            replayedInOrder.iterator.flatten.foreach(_cmd.callback)
            actor.succeed(promise, maxSeqNr)
            actor.transitionTo(None)
          } else {
            reversedMessages.headOption match {
              case None =>
                if (_cmd.toSequenceNr >= highestSeqNr) {
                  replayedInOrder.iterator.flatten.foreach(_cmd.callback)
                  actor.succeed(promise, maxSeqNr)
                  if (!foundMetadata) {
                    actor.metadata = Some(
                      MultiJournalMetadata(
                        highSeqNrs.removed(actor.hotJournalId).updated(currentJournal, highestSeqNr)))
                  }
                  actor.transitionTo(None)
                } else if (isColdestJournal) {
                  replayedInOrder.iterator.flatten.foreach(_cmd.callback)
                  actor.succeed(promise, maxSeqNr)

                  if (!foundMetadata) probeMetaOrTransitionToNone(highestSeqNr)
                  else actor.transitionTo(None)
                } else {
                  // probe meta then transition to next coolest
                  probeMetaAndThen(highestSeqNr, transitionToCooler(actor, Nil, highestSeqNr))
                }

              case Some(lastRepr) =>
                val inOrderFromThisJournal = reversedMessages.reverse
                if (isColdestJournal || inOrderFromThisJournal.head.sequenceNr == _cmd.fromSequenceNr) {
                  (inOrderFromThisJournal.iterator ++ replayedInOrder.iterator.flatten)
                    .take(_cmd.max.min(Int.MaxValue).toInt)
                    .foreach(_cmd.callback)
                  actor.succeed(promise, maxSeqNr)

                  if (!foundMetadata) {
                    if (lastRepr.sequenceNr == highestSeqNr) {
                      actor.metadata =
                        admissibleLatestMetadata.orElse(SomeEmptyMetadata).map(_.updated(currentJournal, highestSeqNr))
                      actor.transitionTo(None)
                    } else probeMetaOrTransitionToNone(highestSeqNr)
                  } else actor.transitionTo(None)
                } else {
                  val transition = transitionToCooler(actor, inOrderFromThisJournal, highestSeqNr)
                  if (!foundMetadata) {
                    if (lastRepr.sequenceNr == highestSeqNr) {
                      actor.metadata = admissibleLatestMetadata.map(_.updated(currentJournal, highestSeqNr))
                      actor.transitionTo(transition())
                    } else {
                      probeMetaAndThen(highestSeqNr, transition)
                    }
                  } else {
                    actor.transitionTo(transition())
                  }
                }
            }
          }

        case JournalProtocol.ReplayedMessage(repr) =>
          // when probing, only the probed event matters
          if (probing.forall(_._1 == repr.sequenceNr)) {
            val (stripped, meta) = stripMultiJournalMetadata(repr)
            if (meta.isEmpty && !isColdestJournal) {
              log.error(
                "Detected persisted event missing metadata in warm journal for [{}], at seqNr [{}].  Stopping per-pid actor",
                pid,
                repr.sequenceNr)
              actor.failAndDrain(promise, MissingExpectedMetadata)
            } else {
              latestMetadata = meta
              if (probing.isEmpty) reversedMessages = stripped :: reversedMessages
            }
          }
      }
    }

    def transitionToCooler(actor: PerPidActor, inOrder: Seq[PersistentRepr], foundHighSeqNr: Long) = {
      assume(directedReplay.fold(journals.size > 1)(_.size > 1))

      () => {
        val foundMetadata = actor.metadata.exists(!_.isEmpty)
        val journalRange = journals.dropRight(1)
        val highSeqs = if (foundHighSeqNr > 0) highSeqNrs.updated(currentJournal, foundHighSeqNr) else highSeqNrs
        val nextReplayedInOrder = {
          val foundByJournal = inOrder.size.toLong +: (replayedInOrder.map(_.size.toLong))
          if (foundByJournal.sum > _cmd.max) {
            // assumes that we have fewer than Int.MaxValue events in memory, which seems like a safe assumption
            Vector(Seq.from((inOrder.iterator ++ replayedInOrder.iterator.flatten).take(_cmd.max.toInt)))
          } else inOrder +: replayedInOrder
        }

        directedReplay match {
          case Some(legs) if legs.size == 1 =>
            nextReplayedInOrder.iterator.flatten.foreach(_cmd.callback)
            val highestSeqNr = highSeqs.lastOption.fold(0L)(_._2)
            actor.succeed(promise, highestSeqNr)
            None

          case Some(legs) =>
            val remainingLegs = legs.dropRight(1)
            val leg = remainingLegs.last
            actor.journals(leg.journalId) ! JournalProtocol.ReplayMessages(
              leg.fromSequenceNr,
              leg.toSequenceNr,
              _cmd.max,
              actor.pid,
              actor.self)
            Some(
              new ReplayingFromCool(
                _cmd,
                journalRange,
                Some(remainingLegs),
                nextReplayedInOrder,
                highSeqs,
                foundMetadata))

          case None =>
            val journalCmd =
              if (_cmd.isMaxSeqNrQuery || _cmd.isReplayOnlyLast) {
                JournalProtocol.ReplayMessages(_cmd.fromSequenceNr, _cmd.toSequenceNr, _cmd.max, actor.pid, actor.self)
              } else {
                val effectiveTo = inOrder.headOption
                  .orElse(replayedInOrder.dropWhile(_.isEmpty).headOption.flatMap(_.headOption))
                  .fold(_cmd.toSequenceNr)(_.sequenceNr - 1)
                JournalProtocol.ReplayMessages(_cmd.fromSequenceNr, effectiveTo, _cmd.max, actor.pid, actor.self)
              }

            actor.journals(journalRange.last) ! journalCmd
            Some(new ReplayingFromCool(_cmd, journalRange, None, nextReplayedInOrder, highSeqs, foundMetadata))
        }
      }
    }

    // if true, not guaranteed that a replayed event will have MultiJournalMetadata
    val currentJournal: Int = directedReplay.fold(journals.last)(_.last.journalId)

    private def promise = _cmd.maxSeqNrPromise
  }

  private final class RecoveringMetadataForWrite(_cmd: WriteOp, journals: Range) extends CurrentAction {
    def behavior(actor: PerPidActor): Actor.Receive = {
      var remainingJournals = journals
      var reversedMessages: List[PersistentRepr] = Nil
      var latestMetadata: Option[MultiJournalMetadata] = None
      var highSeqNrs = SortedMap.empty[Int, Long]

      def currentJournal: Int = remainingJournals.last

      def transitionToNextAction(actor: PerPidActor): Unit = {
        val metadataFromEvent =
          latestMetadata.fold(Map.empty[Int, Long])(actor.admissibleMetaMap(_, currentJournal))
        actor.metadata = Some(MultiJournalMetadata(metadataFromEvent ++ highSeqNrs.removed(actor.hotJournalId)))
        actor.transitionTo(actor.nextActionForWrite(_cmd))
      }

      def transitionToCoolerJournal(actor: PerPidActor): Unit = {
        remainingJournals = remainingJournals.dropRight(1)
        reversedMessages = Nil
        latestMetadata = None
        actor.journals(currentJournal) ! JournalProtocol.ReplayMessages(-1, Long.MaxValue, 1, actor.pid, actor.self)
      }

      {
        case JournalProtocol.ReplayMessagesFailure(cause) =>
          actor.failAndTransition(_cmd.promiseForFailure, cause)

        case JournalProtocol.ReplayedMessage(repr) =>
          reversedMessages = repr :: reversedMessages
          stripMultiJournalMetadata(repr)._2.foreach { metadata =>
            latestMetadata = Some(metadata)
          }

        case JournalProtocol.RecoverySuccess(0) if reversedMessages.nonEmpty =>
          actor.failAndTransition(_cmd.promiseForFailure, JournalProtocolViolation)

        case JournalProtocol.RecoverySuccess(_) if reversedMessages.drop(1).nonEmpty =>
          actor.failAndTransition(_cmd.promiseForFailure, JournalProtocolViolation)

        case JournalProtocol.RecoverySuccess(highestSeqNr) =>
          if (highestSeqNr > 0) {
            highSeqNrs = highSeqNrs.updated(currentJournal, highestSeqNr)
          }

          if (latestMetadata.nonEmpty || remainingJournals.size == 1) {
            transitionToNextAction(actor)
          } else {
            transitionToCoolerJournal(actor)
          }
      }
    }
  }

  private final class DeletingEvents(_cmd: DeleteMessagesTo, journals: Range) extends CurrentAction {
    def behavior(actor: PerPidActor): Actor.Receive = {
      actor.journals(journals.head) ! JournalProtocol.DeleteMessagesTo(actor.pid, _cmd.toSequenceNr, actor.self)

      {
        case DeleteMessagesFailure(cause, _) =>
          actor.fail(_cmd.promise, cause)
          actor.transitionTo(None)

        case _: DeleteMessagesSuccess =>
          if (journals.size == 1) {
            actor.succeed(_cmd.promise, ())
            actor.transitionTo(None)
          } else actor.transitionTo(Some(new DeletingEvents(_cmd, journals.tail)))
      }
    }
  }

  private final class WritingEvents(_cmd: WriteMessages) extends CurrentAction {
    def behavior(actor: PerPidActor): Actor.Receive = {
      val messagesWithMeta = actor.metadata.fold(_cmd.messages) { metadata =>
        _cmd.messages.map { write =>
          val payload = write.payload.map { repr =>
            repr.metadata match {
              case None | Some(_: MultiJournalMetadata) => repr.withMetadata(metadata)
              case Some(CompositeMetadata(entries)) =>
                CompositeMetadata
                  .construct(metadata +: entries.filter(!_.isInstanceOf[MultiJournalMetadata]))
                  .fold(repr)(repr.withMetadata)

              case Some(otherMeta) =>
                repr.withMetadata(new CompositeMetadata(Seq(metadata, otherMeta)))
            }
          }
          AtomicWrite(payload)
        }
      }
      actor.hotJournal ! JournalProtocol.WriteMessages(messagesWithMeta, actor.self, actor.instanceId)
      var promiseResponse: List[Try[Unit]] = Nil
      val remaining = mutable.ListBuffer.from(messagesWithMeta.map(_.payload.size))

      def ack(result: Try[Unit]): Unit =
        if (remaining.head == 1) {
          // Last response for this atomic write... either all succeeded or all failed
          promiseResponse = result :: promiseResponse
          remaining.dropInPlace(1)

          if (remaining.isEmpty) {
            actor.succeed(_cmd.promise, promiseResponse.reverse)
            actor.transitionTo(None)
          }
        } else {
          remaining.update(0, remaining.head - 1)
        }

      {
        case JournalProtocol.WriteMessagesSuccessful =>

        case JournalProtocol.WriteMessagesFailed(cause, _) =>
          actor.fail(_cmd.promise, cause)
          actor.metadata = None
          actor.transitionTo(None)

        case JournalProtocol.WriteMessageSuccess(_, instanceId) if actor.instanceId == instanceId =>
          ack(SuccessUnit)

        case JournalProtocol.WriteMessageRejected(_, cause, instanceId) if actor.instanceId == instanceId =>
          ack(Failure(cause))

        case _: JournalProtocol.WriteMessageSuccess | _: JournalProtocol.WriteMessageRejected |
            _: JournalProtocol.WriteMessageFailure =>
        // no-op
      }
    }
  }

  private final case class PerPidActorStopped(persistenceId: String, actor: ActorRef)
  private final case class JournalStopped(id: Int)
  private final case class PerPidActorRequestsStop(persistenceId: String, actor: ActorRef)
  private final case class DeferredByDrain(persistenceId: String, cmd: PerPidCommand)

  private val SomeEmptyMetadata = Some(MultiJournalMetadata.Empty)
  private val ThunkNone = () => None
  private val SuccessUnit = Success(())
  private val JournalProtocolViolation = new IllegalStateException("JournalProtocol violation detected")
    with NoStackTrace
  private val PerPidActorStopping = new RuntimeException("Per-pid actor is stopping") with NoStackTrace
  private val OperationSuperseded = new RuntimeException("Operation superseded") with NoStackTrace
  private val MissingExpectedMetadata = new IllegalStateException(
    "Detected events in current journal without proper metadata") with NoStackTrace
}

/** INTERNAL API */
private[multijournal] final class MultiJournal(config: Config, cfgPath: String)
    extends AsyncWriteJournal
    with AsyncReplay {
  import MultiJournal._

  val log = Logging(context.system, classOf[MultiJournal])

  val perPidIdleTimeout = config.getDuration("metadata-cache-lifetime").toScala
  def spawnJournals(): Map[Int, ActorRef] = {
    val cfg = config.getObject("journals")
    val (minJournal, maxJournal) = cfg.keySet.asScala.foldLeft(Int.MaxValue -> -1) {
      case ((minJournalId, maxJournalId), candidate) =>
        val kOpt = candidate.toIntOption
        require(kOpt.exists(_ >= 0), s"Journal keys must be non-negative integers, got [$candidate]")

        val cfgVal = cfg.get(candidate)
        cfgVal.unwrapped match {
          case _: String =>
            val k = kOpt.get
            minJournalId.min(k) -> maxJournalId.max(k)

          case _ =>
            throw new IllegalArgumentException(s"Journal values must be strings, got [$cfgVal] for key $candidate")
        }
    }
    require(maxJournal >= minJournal, "At least one journal must be specified")

    val overallConfig = context.system.settings.config
    val pluginConfig = config

    val journals = (minJournal to maxJournal).iterator.map { key =>
      val path = pluginConfig.getString(s"journals.$key")
      require(overallConfig.hasPath(path), s"Journal values must be valid config paths, got [$path] for $key")

      val actor = persistence.journalFor(path)
      val config = persistence.configFor(actor)
      require(
        AllowedJournals(config.getString("class")),
        s"Journal class [${config.getString("class")}] for key $key (path=$path) is not supported by multi-journal")

      key -> actor
    }.toMap

    journals.foreach { case (id, journal) =>
      context.watchWith(journal, JournalStopped(id))
    }
    journals
  }

  val journals =
    try { spawnJournals() }
    catch { case NonFatal(ex) => log.error(ex, "Startup failed"); throw ex }

  val coordinatedShutdown = CoordinatedShutdown(context.system)

  override def receivePluginInternal: Receive = {
    case DeferredByDrain(pid, cmd) =>
      commandToPid(pid, cmd)

    case gm @ GetMetadata(pid, promise) =>
      perPidActors.get(pid) match {
        case None => promise.trySuccess(Map.empty)
        case Some(handle) =>
          handle.lastAccess = Instant.now()
          handle.ref ! gm
      }

    case PerPidActorIdleSince(pid, time, actor) =>
      perPidActors.get(pid) match {
        case Some(handle) if handle.ref == actor =>
          if (handle.lastAccess.isBefore(time)) {
            // ensures that a subsequent command will go to a new per-pid actor
            perPidActors.remove(pid)
            handle.ref ! PoisonPill
          }
        // else, a message was forwarded concurrently with the timeout firing, it will fire again

        case _ =>
      }

    case PerPidActorRequestsStop(persistenceId, actor) =>
      perPidActors.get(persistenceId).filter(_.ref == actor).foreach { _ =>
        perPidActors.remove(persistenceId)
      }
      actor ! PoisonPill

    case PerPidActorStopped(pid, actor) =>
      perPidActors.get(pid).filter(_.ref == actor).foreach { _ =>
        perPidActors.remove(pid)
      }

    case JournalStopped(id) =>
      if (coordinatedShutdown.shutdownReason().isEmpty) {
        log.error("Watched journal (id {}) stopped, shutting down", id)
        context.stop(self)
      }
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] =
    if (messages.isEmpty) Future.successful(Nil)
    else {
      if (messages.size > 1 && messages.iterator.map(_.persistenceId).toSet.size > 1) {
        Future.failed(
          new IllegalArgumentException("Multiple persistence IDs per WriteMessages is not supported") with NoStackTrace)
      } else {
        val promise = Promise[Seq[Try[Unit]]]()
        commandToPid(messages.head.persistenceId, WriteMessages(messages, promise))
        promise.future
      }
    }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val promise = Promise[Unit]()
    commandToPid(persistenceId, DeleteMessagesTo(toSequenceNr, promise))
    promise.future
  }

  override def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Long] = {
    val promise = Promise[Long]()
    commandToPid(persistenceId, ReplayMessages(fromSequenceNr, toSequenceNr, max, recoveryCallback, promise))
    promise.future
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit] = throw new IllegalStateException(
    "asyncReplayMessages is not supposed to be called when implementing AsyncReplay.  This is a bug, please report")

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    throw new IllegalStateException(
      "asyncReadHighestSequenceNr is not supposed to be called when implementing AsyncReplay.  This is a bug, please report")

  private def commandToPid(persistenceId: String, cmd: PerPidCommand): Unit = {
    val actor = perPidActors.get(persistenceId) match {
      case Some(handle) =>
        handle.lastAccess = Instant.now()
        handle.ref

      case None =>
        val handle =
          new PerPidHandle(context.actorOf(perPidHandle(persistenceId, perPidIdleTimeout, journals)), Instant.now())
        perPidActors.update(persistenceId, handle)
        context.watchWith(handle.ref, PerPidActorStopped(persistenceId, handle.ref))
        handle.ref
    }
    actor ! cmd
  }

  private val perPidActors = mutable.Map.empty[String, PerPidHandle]
}
