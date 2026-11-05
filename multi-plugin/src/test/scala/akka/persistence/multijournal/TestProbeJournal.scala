/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import scala.concurrent.Await
import scala.concurrent.CancellationException
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NoStackTrace

import akka.actor.ActorRef
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.annotation.InternalApi
import akka.persistence.AtomicWrite
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncWriteJournal
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestProbeJournal {
  sealed trait Invocation

  case class ReplayMessages(
      pid: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long,
      promise: Promise[Seq[PersistentRepr]])
      extends Invocation

  case class ReadHighestSeqNr(pid: String, fromSequenceNr: Long, promise: Promise[Long]) extends Invocation
  case class WriteMessages(messages: Seq[AtomicWrite], promise: Promise[Seq[Try[Unit]]]) extends Invocation
  case class DeleteMessages(persistenceId: String, toSequenceNr: Long, promise: Promise[Unit]) extends Invocation
  case object JournalStopped extends Invocation

  def config(instanceId: String, atKey: String): Config = ConfigFactory.parseString(s"""
    |$atKey {
    |  class = "${classOf[TestProbeJournal].getName}"
    |  instance-id = "$instanceId"
    |}""".stripMargin)

  def getProbe(instanceId: String): TestProbe[Invocation] =
    Await.result(getAsync(instanceId), 1.second)._1

  def drain(instanceId: String)(implicit timeout: Timeout): Future[Int] = {
    import akka.pattern.ask

    getAsync(instanceId).flatMap { case (_, ref) =>
      (ref ? Flush).mapTo[Int]
    }(ExecutionContext.parasitic)
  }

  private def getAsync(instanceId: String): Future[(TestProbe[Invocation], ActorRef)] =
    synchronized {
      _current.get(instanceId) match {
        case None =>
          val promise = Promise[(TestProbe[Invocation], ActorRef)]()
          _current = _current.updated(instanceId, promise)
          promise.future

        case Some(promise) => promise.future
      }
    }

  private def putProbeAndActor(instanceId: String, probe: TestProbe[Invocation], actor: ActorRef): Unit =
    synchronized {
      _current.get(instanceId) match {
        case None =>
          val promise = Promise[(TestProbe[Invocation], ActorRef)]()
          promise.success(probe -> actor)
          _current = _current.updated(instanceId, promise)

        case Some(promise) =>
          promise.future.value match {
            case None                             => promise.success(probe -> actor)
            case Some(Success((_, currentActor))) => throw new IllegalStateException
            case _                                => throw new IllegalStateException
          }
      }
    }

  private def removeProbe(instanceId: String): Unit =
    synchronized {
      _current.get(instanceId).foreach { promise =>
        promise.tryFailure(new RuntimeException("probe removed"))
        _current = _current.removed(instanceId)
      }
    }

  private case object Flush
  private case class PromiseFinished(promise: Promise[_])
  private case class RecoveryCallback(thunk: () => Unit, whenDone: Promise[Unit])

  private[this] var _current = Map.empty[String, Promise[(TestProbe[Invocation], ActorRef)]]
}

@InternalApi
private[persistence] final class TestProbeJournal(cfg: Config) extends AsyncWriteJournal {
  import TestProbeJournal._

  val instanceId = cfg.getString("instance-id")
  val testProbe = TestProbe[Invocation](s"TestProbeJournal-$instanceId")(context.system.toTyped)

  var pendingPromises = Set.empty[Promise[_]]

  override def postStop(): Unit = {
    val ex = new CancellationException(s"journal $instanceId stopping") with NoStackTrace
    pendingPromises.foreach(_.tryFailure(ex))
    removeProbe(instanceId)
    testProbe.ref ! JournalStopped
    super.postStop()
  }

  override def preStart(): Unit = {
    super.preStart()
    putProbeAndActor(instanceId, testProbe, context.self)
  }

  override def receivePluginInternal: Receive = {
    case Flush =>
      val failed = pendingPromises.foldLeft(0) { (count, promise) =>
        if (promise.tryFailure(new CancellationException("test finished"))) {
          count + 1
        } else count
      }
      pendingPromises = Set.empty

      sender() ! failed

    case PromiseFinished(promise) =>
      if (pendingPromises(promise)) {
        promise.tryFailure(new CancellationException("promise forcefully finished"))
        pendingPromises = pendingPromises.excl(promise)
      }

    case RecoveryCallback(thunk, promise) =>
      thunk()
      promise.trySuccess(())
      pendingPromises = pendingPromises.excl(promise)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    val promise = Promise[Seq[PersistentRepr]]()
    testProbe.ref ! ReplayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, promise)
    val retPromise = Promise[Unit]()
    pendingPromises = pendingPromises ++ Set(promise, retPromise)
    promise.future.onComplete { result =>
      self ! PromiseFinished(promise)
      result match {
        case Success(reprs) =>
          self ! RecoveryCallback(() => reprs.foreach(recoveryCallback), retPromise)
        case Failure(ex) =>
          retPromise.tryFailure(ex)
      }
    }(ExecutionContext.parasitic)
    retPromise.future.onComplete { _ => self ! PromiseFinished(promise) }(ExecutionContext.parasitic)
    retPromise.future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val promise = Promise[Long]()
    testProbe.ref ! ReadHighestSeqNr(persistenceId, fromSequenceNr, promise)
    pendingPromises = pendingPromises.incl(promise)
    promise.future.onComplete { _ => self ! PromiseFinished(promise) }(ExecutionContext.parasitic)
    promise.future
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val promise = Promise[Seq[Try[Unit]]]()
    testProbe.ref ! WriteMessages(messages, promise)
    pendingPromises = pendingPromises.incl(promise)
    promise.future.onComplete { _ => self ! PromiseFinished(promise) }(ExecutionContext.parasitic)
    promise.future
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val promise = Promise[Unit]()
    testProbe.ref ! DeleteMessages(persistenceId, toSequenceNr, promise)
    pendingPromises = pendingPromises.incl(promise)
    promise.future.onComplete { _ => self ! PromiseFinished(promise) }(ExecutionContext.parasitic)
    promise.future
  }
}
