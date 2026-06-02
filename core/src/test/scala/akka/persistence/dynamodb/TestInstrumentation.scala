/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb

import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorSystem

object TestInstrumentationProtocol {
  sealed trait HighLevelContext[OpType] {
    def probe: TestProbe[OpType]
  }

  sealed trait WriteEventsOp
  sealed trait QueryEventsOp
  sealed trait PerEventOp

  case class WriteEventsCalled(
      persistenceId: String,
      firstSequenceNr: Long,
      count: Int,
      probe: TestProbe[WriteEventsOp])
      extends HighLevelContext[WriteEventsOp]

  case class EventsByPersistenceIdCalled(
      persistenceId: String,
      fromSequenceNumber: Long,
      toSequenceNumber: Long,
      correlationId: String,
      probe: TestProbe[QueryEventsOp])
      extends HighLevelContext[QueryEventsOp]

  case class BySliceQueryCalled(entityType: String, slice: Int, correlationId: String, probe: TestProbe[QueryEventsOp])
      extends HighLevelContext[QueryEventsOp]

  case class LoadEventCalled(persistenceId: String, sequenceNumber: Long, probe: TestProbe[QueryEventsOp])
      extends HighLevelContext[QueryEventsOp]

  case class BadContextError(context: AnyRef, probe: TestProbe[Nothing]) extends HighLevelContext[Nothing]

  case class PubSubEventDropped(
      entityType: String,
      persistenceId: String,
      sequenceNumber: Long,
      probe: TestProbe[Nothing])
      extends HighLevelContext[Nothing]

  case class QueryReceivedEvent(persistenceId: String, sequenceNumber: Long, probe: TestProbe[PerEventOp])
      extends QueryEventsOp
  case object BeforeDeserializeEvent extends PerEventOp
  case object AfterDeserializeEvent extends PerEventOp

  case class BeforeSerializeEvent(sequenceNumber: Long) extends WriteEventsOp
  case class AfterSerializeEvent(sequenceNumber: Long) extends WriteEventsOp
}

class TestInstrumentation(system: ClassicActorSystemProvider) extends Instrumentation {
  import TestInstrumentationProtocol._
  import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps

  implicit val typedSystem: ActorSystem[_] = system match {
    case alreadyTyped: ActorSystem[_] => alreadyTyped
    case _                            => system.classicSystem.toTyped
  }

  def probe: TestProbe[HighLevelContext[_]] = _probe
  @volatile private var _probe: TestProbe[HighLevelContext[_]] = TestProbe()

  def resetProbe(): Unit = {
    _probe = TestProbe()
  }

  protected def shouldNotBeCalled(call: String) =
    throw new NotImplementedError(s"$call should not be called")

  protected def badContext(context: AnyRef) =
    probe.ref ! BadContextError(context, TestProbe())

  override def bySliceQueryCalled(entityType: String, slice: Int, correlationId: String): AnyRef = tellProbe(
    BySliceQueryCalled(entityType, slice, correlationId, TestProbe()))

  override def loadEventCalled(persistenceId: String, sequenceNumber: Long): AnyRef = tellProbe(
    LoadEventCalled(persistenceId, sequenceNumber, TestProbe()))

  override def writeEventsCalled(persistenceId: String, firstSequenceNr: Long, count: Int): AnyRef = tellProbe(
    WriteEventsCalled(persistenceId, firstSequenceNr, count, TestProbe()))

  override def eventsByPersistenceIdCalled(
      persistenceId: String,
      fromSequenceNumber: Long,
      toSequenceNumber: Long,
      correlationId: String): AnyRef =
    tellProbe(
      EventsByPersistenceIdCalled(persistenceId, fromSequenceNumber, toSequenceNumber, correlationId, TestProbe()))

  override def pubsubEventDropped(entityType: String, persistenceId: String, sequenceNumber: Long): Unit = tellProbe(
    PubSubEventDropped(entityType, persistenceId, sequenceNumber, TestProbe()))

  override def queryReceivedEvent(persistenceId: String, sequenceNumber: Long, queryContext: AnyRef): AnyRef = {
    val probeFromContext = queryContext match {
      case e: EventsByPersistenceIdCalled => e.probe.ref
      case b: BySliceQueryCalled          => b.probe.ref
      case l: LoadEventCalled             => l.probe.ref
      case _ =>
        badContext(queryContext)
        shouldNotBeCalled("Query received event with probe-missing context")
    }

    val reified = QueryReceivedEvent(persistenceId, sequenceNumber, TestProbe())
    probeFromContext ! reified
    reified
  }

  override def beforeDeserializeEvent(eventContext: AnyRef): Unit =
    perEventOp(eventContext, "Before deserialize event", BeforeDeserializeEvent)

  override def afterDeserializeEvent(eventContext: AnyRef): Unit =
    perEventOp(eventContext, "After deserialize event", AfterDeserializeEvent)

  override def beforeSerializeEvent(sequenceNumber: Long, context: AnyRef): Unit =
    writeEventsOp(context, "Before serialize event", BeforeSerializeEvent(sequenceNumber))

  override def afterSerializeEvent(sequenceNumber: Long, context: AnyRef): Unit =
    writeEventsOp(context, "After serialize event", AfterSerializeEvent(sequenceNumber))

  protected def tellProbe[T <: HighLevelContext[_]](reified: T): T = {
    probe.ref ! reified
    reified
  }

  protected def perEventOp(eventContext: AnyRef, callName: String, reified: => PerEventOp): Unit = {
    val probeFromContext = eventContext match {
      case q: QueryReceivedEvent => q.probe.ref
      case _ =>
        badContext(eventContext)
        shouldNotBeCalled(s"$callName with probe-missing context")
    }

    probeFromContext ! reified
  }

  protected def writeEventsOp(context: AnyRef, callName: String, reified: => WriteEventsOp): Unit = {
    val probeFromContext = context match {
      case w: WriteEventsCalled => w.probe.ref
      case _ =>
        badContext(context)
        shouldNotBeCalled(s"$callName with probe-missing context")
    }

    probeFromContext ! reified
  }
}
