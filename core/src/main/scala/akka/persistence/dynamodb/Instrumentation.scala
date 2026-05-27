/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb

import akka.annotation.InternalApi
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.ActorSystem
import akka.actor.ClassicActorSystemProvider
import scala.util.Success
import org.slf4j.LoggerFactory
import scala.util.Failure

/**
 * Service Provider Interface for instrumenting the operation of the persistence plugin
 *
 * The persistence plugin is allowed to reuse instances of this class and methods may be called from different threads.
 * Accordingly, implementations which mutate their state will need to use some form of concurrency control.
 *
 * Implementations must have a constructor which takes a [[akka.actor.ClassicActorSystemProvider]] as its only
 * parameter. At most one instance of this class will be constructed per actor system.
 */
trait Instrumentation {

  /**
   * Called when a query by-slice stream is run (which might not be when a request is made against DDB)
   *
   * The returned context object is passed to subsequent methods and may be accessed concurrently on different threads
   *
   * @param entityType
   * @param slice
   * @param correlationId
   *   possibly empty correlation ID see [[akka.persistence.query.QueryCorrelationId]]
   * @return
   *   a context object, or [[emptyContext]] (null) if no context object is needed
   */
  def bySliceQueryCalled(entityType: String, slice: Int, correlationId: String): AnyRef

  /**
   * Called when a LoadEventQuery is performed, to load a specific event from the journal
   *
   * The returned context object is passed to subsequent methods and may be accessed concurrently on different threads
   *
   * @return
   *   a context object, or [[emptyContext]] (null) if no context object is needed
   */
  def loadEventCalled(persistenceId: String, sequenceNumber: Long): AnyRef

  /**
   * Called when persisting events.
   *
   * The returned context object is passed to subsequent calls for this event and may be accessed concurrently on
   * different threads.
   *
   * @return
   *   a context object, or [[emptyContext]] (null) if no context object is needed
   */
  def writeEventsCalled(persistenceId: String, firstSequenceNr: Long, count: Int): AnyRef

  /**
   * Called when CurrentEventsByPersistenceIdTypedQuery is performed (either to load a persistent actor for processing
   * commands or by a projection to fill a detected gap)
   *
   * The returned context object is passed to subsequent methods and may be accessed concurrently on different threads
   *
   * @param persistenceId
   * @param fromSequenceNumber
   * @param toSequenceNumber
   * @param correlationId
   *   possibly empty correlation ID see [[akka.persistence.query.QueryCorrelationId]]
   * @return
   *   a context object, or [[emptyContext]] (null) if no context object is needed
   */
  def eventsByPersistenceIdCalled(
      persistenceId: String,
      fromSequenceNumber: Long,
      toSequenceNumber: Long,
      correlationId: String): AnyRef

  /**
   * Called when a query stream receives an event from DynamoDB, before deserializing the payload.
   *
   * The returned context object is passed to subsequent calls for this event and may be accessed concurrently on
   * different threads.
   *
   * @param persistenceId
   * @param sequenceNumber
   * @param queryContext
   *   the potentially null context object returned by a call to bySliceQueryCalled or loadEventCalled or
   *   eventsByPersistenceIdCalled
   * @return
   *   a context object, or [[emptyContext]] (null) if no context object is needed
   */
  def queryReceivedEvent(persistenceId: String, sequenceNumber: Long, queryContext: AnyRef): AnyRef

  /**
   * Called before an event is deserialized
   *
   * @param eventContext
   *   the potentially null context object returned by a call to queryReceivedEvent
   */
  def beforeDeserializeEvent(eventContext: AnyRef): Unit

  /**
   * Called after an event is deserialized
   *
   * @param eventContext
   *   the potentially null context object returned by a call to queryReceivedEvent
   */
  def afterDeserializeEvent(eventContext: AnyRef): Unit

  /** Called when an event is dropped from the pubsub buffer */
  def pubsubEventDropped(entityType: String, persistenceId: String, sequenceNumber: Long): Unit

  /**
   * Called before an event is serialized in the journal
   *
   * @param eventContext
   *   the potentially null context object returned by a call to writeEventsCalled
   */
  def beforeSerializeEvent(sequenceNumber: Long, context: AnyRef): Unit

  /**
   * Called after an event is serialized in the journal
   *
   * @param eventContext
   *   the potentially null context object returned by a call to writeEventsCalled
   */
  def afterSerializeEvent(sequenceNumber: Long, context: AnyRef): Unit
}

/** INTERNAL API: Not for user instantiation */
@InternalApi private[dynamodb] object InstrumentationProvider extends ExtensionId[InstrumentationProvider] {
  def createExtension(system: ActorSystem[_]): InstrumentationProvider = new InstrumentationProvider(system)

  // No Java API: not meant to be called from Java
  private val ConfigPath = "akka.persistence.dynamodb.instrumentation-class"
  private lazy val log = LoggerFactory.getLogger(classOf[InstrumentationProvider])
  private object NoOpInstrumentation extends Instrumentation {
    def emptyContext: AnyRef = null

    def afterDeserializeEvent(eventContext: AnyRef): Unit = {}
    def afterSerializeEvent(sequenceNumber: Long, context: AnyRef): Unit = {}
    def beforeDeserializeEvent(eventContext: AnyRef): Unit = {}
    def beforeSerializeEvent(sequenceNumber: Long, context: AnyRef): Unit = {}
    def bySliceQueryCalled(entityType: String, slice: Int, correlationId: String): AnyRef = emptyContext

    def eventsByPersistenceIdCalled(
        persistenceId: String,
        fromSequenceNumber: Long,
        toSequenceNumber: Long,
        correlationId: String): AnyRef = emptyContext

    def loadEventCalled(persistenceId: String, sequenceNumber: Long): AnyRef = emptyContext
    def pubsubEventDropped(entityType: String, persistenceId: String, sequenceNumber: Long): Unit = {}
    def queryReceivedEvent(persistenceId: String, sequenceNumber: Long, queryContext: AnyRef): AnyRef = emptyContext
    def writeEventsCalled(persistenceId: String, firstSequenceNr: Long, count: Int): AnyRef = emptyContext
  }
}

/**
 * INTERNAL API: Not for user instantiation
 *
 * Creates a single instance of the class whose FQCN is the configured value of
 * `akka.persistence.dynamodb.instrumentation-class`
 */
@InternalApi private[dynamodb] final class InstrumentationProvider(system: ActorSystem[_]) extends Extension {
  import InstrumentationProvider._

  val instrumentation =
    if (system.settings.config.hasPath(ConfigPath)) {
      val fqcn = system.settings.config.getString(ConfigPath)

      if (fqcn.isEmpty) NoOpInstrumentation
      else {
        val maybeInstrumentation = system.dynamicAccess
          .createInstanceFor[Instrumentation](fqcn, List(classOf[ClassicActorSystemProvider] -> system))

        maybeInstrumentation match {
          case Success(instrumentation) => instrumentation
          case Failure(ex) =>
            log.error(
              "Could not create DynamoDB persistence instrumentation [{}], using no-op instrumentation",
              fqcn,
              ex)
            NoOpInstrumentation
        }
      }
    } else NoOpInstrumentation
}
