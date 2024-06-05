/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.projection.BySlicesSourceProvider
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.TestStatusObserver
import akka.projection.TestStatusObserver.Err
import akka.projection.TestStatusObserver.OffsetProgress
import akka.projection.dynamodb.internal.DynamoDBOffsetStore
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Pid
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.SeqNr
import akka.projection.dynamodb.scaladsl.DynamoDBProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object DynamoDBTimestampOffsetProjectionSpec {

  final case class Envelope(id: String, seqNr: Long, message: String)

  /**
   * This variant of TestStatusObserver is useful when the incoming envelope is the original akka projection
   * EventBySliceEnvelope, but we want to assert on [[Envelope]]. The original [[EventEnvelope]] has too many params
   * that are not so interesting for the test including the offset timestamp that would make the it harder to test.
   */
  class DynamoDBTestStatusObserver(
      statusProbe: ActorRef[TestStatusObserver.Status],
      progressProbe: ActorRef[TestStatusObserver.OffsetProgress[Envelope]])
      extends TestStatusObserver[EventEnvelope[String]](statusProbe.ref) {
    override def offsetProgress(projectionId: ProjectionId, envelope: EventEnvelope[String]): Unit =
      progressProbe ! OffsetProgress(
        Envelope(envelope.persistenceId, envelope.sequenceNr, envelope.eventOption.getOrElse("None")))

    override def error(
        projectionId: ProjectionId,
        envelope: EventEnvelope[String],
        cause: Throwable,
        recoveryStrategy: HandlerRecoveryStrategy): Unit =
      statusProbe ! Err(
        Envelope(envelope.persistenceId, envelope.sequenceNr, envelope.eventOption.getOrElse("None")),
        cause)
  }

  class TestTimestampSourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      testSourceProvider: TestSourceProvider[TimestampOffset, EventEnvelope[String]],
      override val maxSlice: Int)
      extends SourceProvider[TimestampOffset, EventEnvelope[String]]
      with BySlicesSourceProvider
      with EventTimestampQuery
      with LoadEventQuery {

    override def source(offset: () => Future[Option[TimestampOffset]]): Future[Source[EventEnvelope[String], NotUsed]] =
      testSourceProvider.source(offset)

    override def extractOffset(envelope: EventEnvelope[String]): TimestampOffset =
      testSourceProvider.extractOffset(envelope)

    override def extractCreationTime(envelope: EventEnvelope[String]): Long =
      testSourceProvider.extractCreationTime(envelope)

    override def minSlice: Int = 0

    override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
      Future.successful(envelopes.collectFirst {
        case env
            if env.persistenceId == persistenceId && env.sequenceNr == sequenceNr && env.offset
              .isInstanceOf[TimestampOffset] =>
          env.offset.asInstanceOf[TimestampOffset].timestamp
      })
    }

    override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
      envelopes.collectFirst {
        case env if env.persistenceId == persistenceId && env.sequenceNr == sequenceNr =>
          env.asInstanceOf[EventEnvelope[Event]]
      } match {
        case Some(env) => Future.successful(env)
        case None =>
          Future.failed(
            new NoSuchElementException(
              s"Event with persistenceId [$persistenceId] and sequenceNr [$sequenceNr] not found."))
      }
    }
  }

  // test model is as simple as a text that gets other string concatenated to it
  case class ConcatStr(id: String, text: String) {
    def concat(newMsg: String): ConcatStr = {
      if (text == "")
        copy(id, newMsg)
      else
        copy(text = text + "|" + newMsg)
    }
  }

  final case class TestRepository()(implicit ec: ExecutionContext, system: ActorSystem[_]) {
    private val store = new ConcurrentHashMap[String, String]()

    private val logger = LoggerFactory.getLogger(this.getClass)

    def concatToText(id: String, payload: String): Future[Done] = {
      val savedStrOpt = findById(id)

      savedStrOpt.flatMap { strOpt =>
        val newConcatStr = strOpt
          .map {
            _.concat(payload)
          }
          .getOrElse(ConcatStr(id, payload))

        upsert(newConcatStr)
      }
    }

    def update(id: String, payload: String): Future[Done] = {
      upsert(ConcatStr(id, payload))
    }

    def updateWithNullValue(id: String): Future[Done] = {
      upsert(ConcatStr(id, null))
    }

    private def upsert(concatStr: ConcatStr): Future[Done] = {
      logger.debug("TestRepository.upsert: [{}]", concatStr)
      store.put(concatStr.id, concatStr.text)
      Future.successful(Done)
    }

    def findById(id: String): Future[Option[ConcatStr]] = {
      logger.debug("TestRepository.findById: [{}]", id)
      Future.successful(Option(store.get(id)).map(text => ConcatStr(id, text)))
    }

  }

}

class DynamoDBTimestampOffsetProjectionSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import DynamoDBTimestampOffsetProjectionSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  private val logger = LoggerFactory.getLogger(getClass)
  private def createOffsetStore(
      projectionId: ProjectionId,
      sourceProvider: TestTimestampSourceProvider): DynamoDBOffsetStore =
    new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)
  private val projectionTestKit = ProjectionTestKit(system)

  private val repository = new TestRepository()

  def createSourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      complete: Boolean = true): TestTimestampSourceProvider = {
    val sp = TestSourceProvider[TimestampOffset, EventEnvelope[String]](
      Source(envelopes),
      _.offset.asInstanceOf[TimestampOffset])
      .withStartSourceFrom { (lastProcessedOffset, offset) =>
        offset.timestamp.isBefore(lastProcessedOffset.timestamp) ||
        (offset.timestamp == lastProcessedOffset.timestamp && offset.seen == lastProcessedOffset.seen)
      }
      .withAllowCompletion(complete)

    new TestTimestampSourceProvider(envelopes, sp, persistenceExt.numberOfSlices - 1)
  }

  def createBacktrackingSourceProvider(
      envelopes: immutable.IndexedSeq[EventEnvelope[String]],
      complete: Boolean = true): TestTimestampSourceProvider = {
    val sp = TestSourceProvider[TimestampOffset, EventEnvelope[String]](
      Source(envelopes),
      _.offset.asInstanceOf[TimestampOffset])
      .withStartSourceFrom { (_, _) => false } // include all
      .withAllowCompletion(complete)
    new TestTimestampSourceProvider(envelopes, sp, persistenceExt.numberOfSlices - 1)
  }

  private def offsetShouldBe[Offset](expected: Offset)(implicit offsetStore: DynamoDBOffsetStore) = {
    val expectedTimestampOffset = expected.asInstanceOf[TimestampOffset]
    offsetStore.readOffset[TimestampOffset]().futureValue
    // FIXME tests are assuming latestOffset, but DynamoDBOffsetStore returns latest from earliest slice
    offsetStore.getState().latestOffset shouldBe expectedTimestampOffset
    // FIXME from r2dbc
//    val offset = offsetStore.readOffset[TimestampOffset]().futureValue
//    offset shouldBe Some(
//      TimestampOffset(
//        expectedTimestampOffset.timestamp,
//        readTimestamp = Instant.EPOCH,
//        seen = expectedTimestampOffset.seen))
  }

  private def offsetShouldBeEmpty()(implicit offsetStore: DynamoDBOffsetStore) = {
    offsetStore.readOffset[TimestampOffset]().futureValue shouldBe empty
  }

  private def projectedValueShouldBe(expected: String)(implicit entityId: String) = {
    val opt = repository.findById(entityId).futureValue.map(_.text)
    opt shouldBe Some(expected)
  }

  // TODO: extract this to some utility
  @tailrec private def eventuallyExpectError(sinkProbe: TestSubscriber.Probe[_]): Throwable = {
    sinkProbe.expectNextOrError() match {
      case Right(_)  => eventuallyExpectError(sinkProbe)
      case Left(exc) => exc
    }
  }

  private val concatHandlerFail4Msg = "fail on fourth envelope"

  class ConcatHandler(repository: TestRepository, failPredicate: EventEnvelope[String] => Boolean = _ => false)
      extends Handler[EventEnvelope[String]] {

    private val logger = LoggerFactory.getLogger(getClass)
    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(envelope: EventEnvelope[String]): Future[Done] = {
      if (failPredicate(envelope)) {
        _attempts.incrementAndGet()
        throw TestException(concatHandlerFail4Msg + s" after $attempts attempts")
      } else {
        logger.debug(s"handling {}", envelope)
        repository.concatToText(envelope.persistenceId, envelope.event)
      }
    }

  }

  private val clock = TestClock.nowMicros()
  def tick(): TestClock = {
    clock.tick(JDuration.ofMillis(1))
    clock
  }

  def createEnvelope(pid: Pid, seqNr: SeqNr, timestamp: Instant, event: String): EventEnvelope[String] = {
    val entityType = PersistenceId.extractEntityType(pid)
    val slice = persistenceExt.sliceForPersistenceId(pid)
    EventEnvelope(
      TimestampOffset(timestamp, timestamp.plusMillis(1000), Map(pid -> seqNr)),
      pid,
      seqNr,
      event,
      timestamp.toEpochMilli,
      entityType,
      slice)
  }

  def backtrackingEnvelope(env: EventEnvelope[String]): EventEnvelope[String] =
    new EventEnvelope[String](
      env.offset,
      env.persistenceId,
      env.sequenceNr,
      eventOption = None,
      env.timestamp,
      env.eventMetadata,
      env.entityType,
      env.slice,
      env.filtered,
      source = EnvelopeOrigin.SourceBacktracking)

  def createEnvelopes(pid: Pid, numberOfEvents: Int): immutable.IndexedSeq[EventEnvelope[String]] = {
    (1 to numberOfEvents).map { n =>
      createEnvelope(pid, n, tick().instant(), s"e$n")
    }
  }

  def createEnvelopesWithDuplicates(pid1: Pid, pid2: Pid): Vector[EventEnvelope[String]] = {
    val startTime = TestClock.nowMicros().instant()

    Vector(
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 1, startTime.plusMillis(2), s"e2-1"),
      createEnvelope(pid1, 3, startTime.plusMillis(4), s"e1-3"),
      // pid1-3 is emitted before pid2-2 even though pid2-2 timestamp is earlier,
      // from backtracking query previous events are emitted again, including the missing pid2-2
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 1, startTime.plusMillis(2), s"e2-1"),
      // now it pid2-2 is included
      createEnvelope(pid2, 2, startTime.plusMillis(3), s"e2-2"),
      createEnvelope(pid1, 3, startTime.plusMillis(4), s"e1-3"),
      // and then some normal again
      createEnvelope(pid1, 4, startTime.plusMillis(5), s"e1-4"),
      createEnvelope(pid2, 3, startTime.plusMillis(6), s"e2-3"))
  }

  def createEnvelopesUnknownSequenceNumbers(startTime: Instant, pid1: Pid, pid2: Pid): Vector[EventEnvelope[String]] = {
    Vector(
      createEnvelope(pid1, 1, startTime, s"e1-1"),
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 1, startTime.plusMillis(2), s"e2-1"),
      // pid2 seqNr 2 missing, will reject 3
      createEnvelope(pid2, 3, startTime.plusMillis(4), s"e2-3"),
      createEnvelope(pid1, 3, startTime.plusMillis(5), s"e1-3"),
      // pid1 seqNr 4 missing, will reject 5
      createEnvelope(pid1, 5, startTime.plusMillis(7), s"e1-5"))
  }

  def createEnvelopesBacktrackingUnknownSequenceNumbers(
      startTime: Instant,
      pid1: Pid,
      pid2: Pid): Vector[EventEnvelope[String]] = {
    Vector(
      // may also contain some duplicates
      createEnvelope(pid1, 2, startTime.plusMillis(1), s"e1-2"),
      createEnvelope(pid2, 2, startTime.plusMillis(3), s"e2-2"),
      createEnvelope(pid2, 3, startTime.plusMillis(4), s"e2-3"),
      createEnvelope(pid1, 3, startTime.plusMillis(5), s"e1-3"),
      createEnvelope(pid1, 4, startTime.plusMillis(6), s"e1-4"),
      createEnvelope(pid1, 5, startTime.plusMillis(7), s"e1-5"),
      createEnvelope(pid2, 4, startTime.plusMillis(8), s"e2-4"),
      createEnvelope(pid1, 6, startTime.plusMillis(9), s"e1-6"))
  }

  def markAsFilteredEvent[A](env: EventEnvelope[A]): EventEnvelope[A] = {
    new EventEnvelope[A](
      env.offset,
      env.persistenceId,
      env.sequenceNr,
      env.eventOption,
      env.timestamp,
      env.eventMetadata,
      env.entityType,
      env.slice,
      filtered = true,
      env.source)
  }

  "A DynamoDB at-least-once projection with TimestampOffset" must {

    "persist projection and offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.atLeastOnceAsync(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new ConcatHandler(repository))

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }
      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
    }

    "filter duplicates" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.atLeastOnceAsync(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new ConcatHandler(repository))

      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3")(pid2)
      }
      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
    }

    "filter out unknown sequence numbers" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val startTime = TestClock.nowMicros().instant()
      val sourceProvider = new TestSourceProviderWithInput()
      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val projectionRef = spawn(ProjectionBehavior(DynamoDBProjection
        .atLeastOnceAsync(projectionId, Some(settings), sourceProvider, handler = () => new ConcatHandler(repository))))
      val input = sourceProvider.input.futureValue

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes1.foreach(input ! _)

      eventually {
        projectedValueShouldBe("e1-1|e1-2|e1-3")(pid1)
        projectedValueShouldBe("e2-1")(pid2)
      }

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes2.foreach(input ! _)

      eventually {
        projectedValueShouldBe("e1-1|e1-2|e1-3|e1-4|e1-5|e1-6")(pid1)
        projectedValueShouldBe("e2-1|e2-2|e2-3|e2-4")(pid2)
      }

      eventually {
        offsetShouldBe(envelopes2.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "re-delivery inflight events after failure with retry recovery strategy" in {
      implicit val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid1, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val failOnce = new AtomicBoolean(true)
      val failPredicate = (ev: EventEnvelope[String]) => {
        // fail on first call for event 4, let it pass afterwards
        ev.sequenceNr == 4 && failOnce.compareAndSet(true, false)
      }
      val bogusEventHandler = new ConcatHandler(repository, failPredicate)

      val projectionFailing =
        DynamoDBProjection
          .atLeastOnceAsync(projectionId, Some(settings), sourceProvider, handler = () => bogusEventHandler)
          .withSaveOffset(afterEnvelopes = 5, afterDuration = 2.seconds)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(2, 10.millis))

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        projectedValueShouldBe("e1|e2|e3|e4|e5|e6")
      }

      bogusEventHandler.attempts shouldBe 1

      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
    }

    "be able to skip envelopes but still store offset" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6).map { env =>
        if (env.event == "e3" || env.event == "e4" || env.event == "e6")
          markAsFilteredEvent(env)
        else
          env
      }
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.atLeastOnceAsync(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new ConcatHandler(repository))

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e5")
      }
      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
    }

    "handle async projection" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val result = new StringBuffer()

      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          Future
            .successful {
              result.append(envelope.event).append("|")
            }
            .map(_ => Done)
        }
      }

      val projection =
        DynamoDBProjection.atLeastOnceAsync(projectionId, Some(settings), sourceProvider, handler = () => handler())

      projectionTestKit.run(projection) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }
      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
    }

    "re-delivery inflight events after failure with retry recovery strategy for async projection" in {
      implicit val pid1 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopes(pid1, 6)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val failOnce = new AtomicBoolean(true)
      val failPredicate = (ev: EventEnvelope[String]) => {
        // fail on first call for event 4, let it pass afterwards
        ev.sequenceNr == 4 && failOnce.compareAndSet(true, false)
      }

      val result = new StringBuffer()
      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          if (failPredicate(envelope)) {
            throw TestException(s"failed to process event '${envelope.sequenceNr}'")
          } else {
            Future
              .successful {
                result.append(envelope.event).append("|")
              }
              .map(_ => Done)
          }
        }
      }

      val projectionFailing =
        DynamoDBProjection
          .atLeastOnceAsync(projectionId, Some(settings), sourceProvider, handler = () => handler())
          .withSaveOffset(afterEnvelopes = 5, afterDuration = 2.seconds)
          .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndSkip(2, 10.millis))

      offsetShouldBeEmpty()
      projectionTestKit.run(projectionFailing) {
        result.toString shouldBe "e1|e2|e3|e4|e5|e6|"
      }

      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
    }

    "filter duplicates for async projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val envelopes = createEnvelopesWithDuplicates(pid1, pid2)
      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val result1 = new StringBuffer()
      val result2 = new StringBuffer()

      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          Future
            .successful {
              if (envelope.persistenceId == pid1)
                result1.append(envelope.event).append("|")
              else
                result2.append(envelope.event).append("|")
            }
            .map(_ => Done)
        }
      }

      val projection =
        DynamoDBProjection.atLeastOnceAsync(projectionId, Some(settings), sourceProvider, handler = () => handler())

      projectionTestKit.run(projection) {
        result1.toString shouldBe "e1-1|e1-2|e1-3|e1-4|"
        result2.toString shouldBe "e2-1|e2-2|e2-3|"
      }
      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
    }

    "filter out unknown sequence numbers for async projection" in {
      val pid1 = UUID.randomUUID().toString
      val pid2 = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()
      val startTime = TestClock.nowMicros().instant()
      val sourceProvider = new TestSourceProviderWithInput()
      implicit val offsetStore: DynamoDBOffsetStore =
        new DynamoDBOffsetStore(projectionId, Some(sourceProvider), system, settings, client)

      val result1 = new StringBuffer()
      val result2 = new StringBuffer()

      def handler(): Handler[EventEnvelope[String]] = new Handler[EventEnvelope[String]] {
        override def process(envelope: EventEnvelope[String]): Future[Done] = {
          Future
            .successful {
              if (envelope.persistenceId == pid1)
                result1.append(envelope.event).append("|")
              else
                result2.append(envelope.event).append("|")
            }
            .map(_ => Done)
        }
      }

      val projectionRef = spawn(
        ProjectionBehavior(
          DynamoDBProjection.atLeastOnceAsync(projectionId, Some(settings), sourceProvider, handler = () => handler())))
      val input = sourceProvider.input.futureValue

      val envelopes1 = createEnvelopesUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes1.foreach(input ! _)

      eventually {
        result1.toString shouldBe "e1-1|e1-2|e1-3|"
        result2.toString shouldBe "e2-1|"
      }

      // simulate backtracking
      logger.debug("Starting backtracking")
      val envelopes2 = createEnvelopesBacktrackingUnknownSequenceNumbers(startTime, pid1, pid2)
      envelopes2.foreach(input ! _)

      eventually {
        result1.toString shouldBe "e1-1|e1-2|e1-3|e1-4|e1-5|e1-6|"
        result2.toString shouldBe "e2-1|e2-2|e2-3|e2-4|"
      }

      eventually {
        offsetShouldBe(envelopes2.last.offset)
      }
      projectionRef ! ProjectionBehavior.Stop
    }

    "be able to skip envelopes but still store offset for async projection" in {
      implicit val pid = UUID.randomUUID().toString
      val projectionId = genRandomProjectionId()

      val envelopes = createEnvelopes(pid, 6).map { env =>
        if (env.event == "e3" || env.event == "e4" || env.event == "e6")
          markAsFilteredEvent(env)
        else
          env
      }

      val sourceProvider = createSourceProvider(envelopes)
      implicit val offsetStore = createOffsetStore(projectionId, sourceProvider)

      val projection =
        DynamoDBProjection.atLeastOnceAsync(
          projectionId,
          Some(settings),
          sourceProvider,
          handler = () => new ConcatHandler(repository))

      offsetShouldBeEmpty()
      projectionTestKit.run(projection) {
        projectedValueShouldBe("e1|e2|e5")
      }
      eventually {
        offsetShouldBe(envelopes.last.offset)
      }
    }
  }

  // FIXME see more tests in R2dbcTimestampOffsetProjectionSpec

}
