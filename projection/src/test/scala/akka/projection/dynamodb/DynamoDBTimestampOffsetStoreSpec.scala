/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.dynamodb.internal.DynamoDBOffsetStore
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Pid
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.SeqNr
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation.Accepted
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation.Duplicate
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation.RejectedBacktrackingSeqNr
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Validation.RejectedSeqNr
import akka.projection.dynamodb.internal.OffsetPidSeqNr
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object DynamoDBTimestampOffsetStoreSpec {
  class TestTimestampSourceProvider(override val minSlice: Int, override val maxSlice: Int, clock: TestClock)
      extends BySlicesSourceProvider
      with EventTimestampQuery
      with LoadEventQuery {

    override def timestampOf(persistenceId: String, sequenceNr: SeqNr): Future[Option[Instant]] =
      Future.successful(Some(clock.instant()))

    override def loadEnvelope[Event](persistenceId: String, sequenceNr: SeqNr): Future[EventEnvelope[Event]] =
      throw new IllegalStateException("loadEvent shouldn't be used here")
  }
}

class DynamoDBTimestampOffsetStoreSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory
        .parseString("""
    # to be able to test eviction
    akka.projection.dynamodb.offset-store.keep-number-of-entries = 0
    """).withFallback(TestConfig.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import DynamoDBTimestampOffsetStoreSpec.TestTimestampSourceProvider

  override def typedSystem: ActorSystem[_] = system

  private val clock = TestClock.nowMicros()
  def tick(): Unit = clock.tick(JDuration.ofMillis(1))

  private val log = LoggerFactory.getLogger(getClass)

  private def createOffsetStore(
      projectionId: ProjectionId,
      customSettings: DynamoDBProjectionSettings = settings,
      eventTimestampQueryClock: TestClock = clock) =
    new DynamoDBOffsetStore(
      projectionId,
      Some(new TestTimestampSourceProvider(0, persistenceExt.numberOfSlices - 1, eventTimestampQueryClock)),
      system,
      customSettings,
      client)

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

  def filteredEnvelope(env: EventEnvelope[String]): EventEnvelope[String] =
    new EventEnvelope[String](
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

  def createUpdatedDurableState(
      pid: Pid,
      revision: SeqNr,
      timestamp: Instant,
      state: String): UpdatedDurableState[String] =
    new UpdatedDurableState(
      pid,
      revision,
      state,
      TimestampOffset(timestamp, timestamp.plusMillis(1000), Map(pid -> revision)),
      timestamp.toEpochMilli)

  def slice(pid: String): Int =
    persistenceExt.sliceForPersistenceId(pid)

  s"The DynamoDBOffsetStore for TimestampOffset" must {

    "save TimestampOffset with one entry" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffset]().futureValue
      readOffset1 shouldBe Some(offset1)
      offsetStore.getState().offsetBySlice(slice("p1")) shouldBe offset1
      offsetStore.storedSeqNr("p1").futureValue shouldBe 3L

      tick()
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 4L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p1", 4L)).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]().futureValue
      readOffset2 shouldBe Some(offset2) // yep, saveOffset overwrites previous
      offsetStore.getState().offsetBySlice(slice("p1")) shouldBe offset2
      offsetStore.storedSeqNr("p1").futureValue shouldBe 4L
    }

    "save TimestampOffset with several seen entries" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      val entityType = nextEntityType()
      val p1 = nextPersistenceId(entityType)
      val s = slice(p1.id)
      val p2 = randomPersistenceIdForSlice(entityType, s)
      val p3 = randomPersistenceIdForSlice(entityType, s)
      val p4 = randomPersistenceIdForSlice(entityType, s)

      tick()
      val offset1a = TimestampOffset(clock.instant(), Map(p1.id -> 3L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1a, p1.id, 3L)).futureValue
      val offset1b = TimestampOffset(clock.instant(), Map(p1.id -> 3L, p2.id -> 1L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1b, p2.id, 1L)).futureValue
      val offset1c = TimestampOffset(clock.instant(), Map(p1.id -> 3L, p2.id -> 1L, p3.id -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1c, p3.id, 5L)).futureValue

      val readOffset1 = offsetStore.readOffset[TimestampOffset]().futureValue
      val expectedOffset1 =
        TimestampOffset(offset1a.timestamp, offset1a.readTimestamp, Map(p1.id -> 3L, p2.id -> 1L, p3.id -> 5L))
      readOffset1 shouldBe Some(expectedOffset1)

      tick()
      val offset2 = TimestampOffset(clock.instant(), Map(p1.id -> 4L, p3.id -> 6L, p4.id -> 9L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, p3.id, 6L)).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]().futureValue
      // note that it's not the seen Map in the saveOffset that is used, but the pid, seqNr of saveOffset,
      // so here we have only saved p3 -> 6
      val expectedOffset2 = TimestampOffset(offset2.timestamp, offset2.readTimestamp, Map(p3.id -> 6L))
      readOffset2 shouldBe Some(expectedOffset2)
    }

    "save TimestampOffset when same timestamp" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)
      val entityType = nextEntityType()
      val p1 = nextPersistenceId(entityType)
      val s = slice(p1.id)
      val p2 = randomPersistenceIdForSlice(entityType, s)
      val p3 = randomPersistenceIdForSlice(entityType, s)
      val p4 = randomPersistenceIdForSlice(entityType, s)

      tick()
      // the seen map in saveOffset is not used when saving, so using empty Map for simplicity
      val offset1 = TimestampOffset(clock.instant(), Map.empty)
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, p1.id, 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, p2.id, 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, p3.id, 5L)).futureValue
      offsetStore.readOffset[TimestampOffset]().futureValue
      val expectedOffset1 = TimestampOffset(clock.instant(), Map(p1.id -> 3L, p2.id -> 1L, p3.id -> 5L))
      offsetStore.getState().offsetBySlice(s) shouldBe expectedOffset1

      // not tick, same timestamp
      val offset2 = TimestampOffset(clock.instant(), Map.empty)
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, p2.id, 2L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, p4.id, 9L)).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]().futureValue
      val expectedOffset2 = TimestampOffset(clock.instant(), expectedOffset1.seen ++ Map(p2.id -> 2L, p4.id -> 9L))
      // all should be included since same timestamp
      readOffset2 shouldBe Some(expectedOffset2)

      // saving new with later timestamp
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map(p1.id -> 4L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset3, p1.id, 4L)).futureValue
      val readOffset3 = offsetStore.readOffset[TimestampOffset]().futureValue
      readOffset3 shouldBe Some(offset3)
      offsetStore.getState().offsetBySlice(s) shouldBe offset3
    }

    "save batch of TimestampOffsets" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val p1 = "p1"
      val slice1 = persistenceExt.sliceForPersistenceId(p1)
      slice1 shouldBe 449

      val p2 = "p2"
      val slice2 = persistenceExt.sliceForPersistenceId(p2)
      slice2 shouldBe 450

      val p3 = "p10"
      val slice3 = persistenceExt.sliceForPersistenceId(p3)
      slice3 shouldBe 655

      val p4 = "p11"
      val slice4 = persistenceExt.sliceForPersistenceId(p4)
      slice4 shouldBe 656

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map.empty)
      tick()
      val offset2 = TimestampOffset(clock.instant(), Map.empty)
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map.empty)
      tick()
      val offset4 = TimestampOffset(clock.instant(), Map.empty)
      val offsetsBatch1 = Vector(
        OffsetPidSeqNr(offset1, p1, 3L),
        OffsetPidSeqNr(offset1, p2, 1L),
        OffsetPidSeqNr(offset1, p3, 5L),
        OffsetPidSeqNr(offset2, p4, 1L),
        OffsetPidSeqNr(offset2, p1, 4L),
        OffsetPidSeqNr(offset3, p2, 2L))

      offsetStore.saveOffsets(offsetsBatch1).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffset]().futureValue
      // FIXME could be relevant if we use moreThanOneProjectionKey, otherwise remove
//      readOffset1 shouldBe Some(withoutSeen(offsetsBatch1.last.offset))
      offsetStore.getState().offsetBySlice(slice1) shouldBe TimestampOffset(offset2.timestamp, Map(p1 -> 4L))
      offsetStore.getState().offsetBySlice(slice2) shouldBe TimestampOffset(offset3.timestamp, Map(p2 -> 2L))
      offsetStore.getState().offsetBySlice(slice3) shouldBe TimestampOffset(offset1.timestamp, Map(p3 -> 5L))
      offsetStore.getState().offsetBySlice(slice4) shouldBe TimestampOffset(offset2.timestamp, Map(p4 -> 1L))

      offsetStore.load(Vector(p1, p2, p3, p4)).futureValue
      offsetStore.getState().byPid(p1).seqNr shouldBe 4L
      offsetStore.getState().byPid(p2).seqNr shouldBe 2L
      offsetStore.getState().byPid(p3).seqNr shouldBe 5L
      offsetStore.getState().byPid(p4).seqNr shouldBe 1L

      tick()
      val offset5 = TimestampOffset(clock.instant(), Map(p1 -> 5L))
      offsetStore.saveOffsets(Vector(OffsetPidSeqNr(offset5, p1, 5L))).futureValue

      tick()
      // duplicate
      val offset6 = TimestampOffset(clock.instant(), Map(p2 -> 1L))
      offsetStore.saveOffsets(Vector(OffsetPidSeqNr(offset6, p2, 1L))).futureValue

      tick()
      val offset7 = TimestampOffset(clock.instant(), Map(p1 -> 6L))
      tick()
      val offset8 = TimestampOffset(clock.instant(), Map(p1 -> 7L))
      tick()
      val offset9 = TimestampOffset(clock.instant(), Map(p1 -> 8L))
      val offsetsBatch2 =
        Vector(OffsetPidSeqNr(offset7, p1, 6L), OffsetPidSeqNr(offset8, p1, 7L), OffsetPidSeqNr(offset9, p1, 8L))

      offsetStore.saveOffsets(offsetsBatch2).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]().futureValue
      // FIXME could be relevant if we use moreThanOneProjectionKey, otherwise remove
      //readOffset2 shouldBe Some(withoutSeen(offsetsBatch2.last.offset))
      offsetStore.load(Vector(p1, p2, p3, p4)).futureValue
      offsetStore.getState().byPid(p1).seqNr shouldBe 8L
      offsetStore.getState().byPid(p2).seqNr shouldBe 2L // duplicate with lower seqNr not saved
      offsetStore.getState().byPid(p3).seqNr shouldBe 5L
      offsetStore.getState().byPid(p4).seqNr shouldBe 1L
    }

    "save batch of many TimestampOffsets" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      def test(pidPrefix: String, numberOfOffsets: Int): Unit = {
        withClue(s"with $numberOfOffsets offsets: ") {
          val offsetsBatch = (1 to numberOfOffsets).map { n =>
            tick()
            val offset = TimestampOffset(clock.instant(), Map.empty)
            OffsetPidSeqNr(offset, s"$pidPrefix$n", n)
          }
          offsetStore.saveOffsets(offsetsBatch).futureValue
          offsetStore.readOffset[TimestampOffset]().futureValue
          (1 to numberOfOffsets).map { n =>
            val pid = s"$pidPrefix$n"
            offsetStore.load(pid).futureValue
            offsetStore.getState().byPid(pid).seqNr shouldBe n
          }
        }
      }

      test("a", settings.offsetBatchSize)
      test("a", settings.offsetBatchSize - 1)
      test("a", settings.offsetBatchSize + 1)
      test("a", settings.offsetBatchSize * 2)
      test("a", settings.offsetBatchSize * 2 - 1)
      test("a", settings.offsetBatchSize * 2 + 1)
    }

    "perf save batch of TimestampOffsets" in {
      // FIXME Too many items requested for the BatchWriteItem
      pending
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val warmupIterations = 1 // increase this for serious testing
      val iterations = 2000 // increase this for serious testing
      val batchSize = 100

      // warmup
      (1 to warmupIterations).foreach { _ =>
        val offsets = (1 to batchSize).map { n =>
          val offset = TimestampOffset(Instant.now(), Map(s"p$n" -> 1L))
          OffsetPidSeqNr(offset, s"p$n", 1L)
        }
        Await.result(offsetStore.saveOffsets(offsets), 5.seconds)
      }

      val totalStartTime = System.nanoTime()
      var startTime = System.nanoTime()
      var count = 0

      (1 to iterations).foreach { i =>
        val offsets = (1 to batchSize).map { n =>
          val offset = TimestampOffset(Instant.now(), Map(s"p$n" -> 1L))
          OffsetPidSeqNr(offset, s"p$n", 1L)
        }
        count += batchSize
        Await.result(offsetStore.saveOffsets(offsets), 5.seconds)

        if (i % 1000 == 0) {
          val totalDurationMs = (System.nanoTime() - totalStartTime) / 1000 / 1000
          val durationMs = (System.nanoTime() - startTime) / 1000 / 1000
          println(
            s"#${i * batchSize}: $count took $durationMs ms, RPS ${1000L * count / durationMs}, Total RPS ${1000L * i * batchSize / totalDurationMs}")
          startTime = System.nanoTime()
          count = 0
        }
      }
    }

    "not update when earlier seqNr" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      val readOffset1 = offsetStore.readOffset[TimestampOffset]().futureValue
      readOffset1 shouldBe Some(offset1)

      clock.setInstant(clock.instant().minusMillis(1))
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 2L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p1", 2L)).futureValue
      val readOffset2 = offsetStore.readOffset[TimestampOffset]().futureValue
      readOffset2 shouldBe Some(offset1) // keeping offset1
    }

    "readOffset from given slices" in {
      val projectionId0 = ProjectionId(UUID.randomUUID().toString, "0-1023")
      val projectionId1 = ProjectionId(projectionId0.name, "0-511")
      val projectionId2 = ProjectionId(projectionId0.name, "512-1023")

      val p1 = "p1"
      val slice1 = persistenceExt.sliceForPersistenceId(p1)
      slice1 shouldBe 449

      val p2 = "p2"
      val slice2 = persistenceExt.sliceForPersistenceId(p2)
      slice2 shouldBe 450

      val p3 = "p10"
      val slice3 = persistenceExt.sliceForPersistenceId(p3)
      slice3 shouldBe 655

      val p4 = "p11"
      val slice4 = persistenceExt.sliceForPersistenceId(p4)
      slice4 shouldBe 656

      val offsetStore0 =
        new DynamoDBOffsetStore(
          projectionId0,
          Some(new TestTimestampSourceProvider(0, persistenceExt.numberOfSlices - 1, clock)),
          system,
          settings,
          client)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map(p1 -> 3L))
      offsetStore0.saveOffset(OffsetPidSeqNr(offset1, p1, 3L)).futureValue
      tick()
      val offset2 = TimestampOffset(clock.instant(), Map(p2 -> 4L))
      offsetStore0.saveOffset(OffsetPidSeqNr(offset2, p2, 4L)).futureValue
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map(p3 -> 7L))
      offsetStore0.saveOffset(OffsetPidSeqNr(offset3, p3, 7L)).futureValue
      tick()
      val offset4 = TimestampOffset(clock.instant(), Map(p4 -> 5L))
      offsetStore0.saveOffset(OffsetPidSeqNr(offset4, p4, 5L)).futureValue

      val offsetStore1 =
        new DynamoDBOffsetStore(
          projectionId1,
          Some(new TestTimestampSourceProvider(0, 511, clock)),
          system,
          settings,
          client)
      offsetStore1.readOffset().futureValue
      // FIXME this is not really testing anything, the test is supposed to test that it is responsible for a range
      offsetStore1.load(Vector(p1, p2)).futureValue
      offsetStore1.getState().byPid.keySet shouldBe Set(p1, p2)

      val offsetStore2 =
        new DynamoDBOffsetStore(
          projectionId2,
          Some(new TestTimestampSourceProvider(512, 1023, clock)),
          system,
          settings,
          client)
      offsetStore2.readOffset().futureValue
      // FIXME this is not really testing anything, the test is supposed to test that it is responsible for a range
      offsetStore2.load(Vector(p3, p4)).futureValue
      offsetStore2.getState().byPid.keySet shouldBe Set(p3, p4)
    }

    "filter duplicates" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      tick()
      val offset1 = TimestampOffset(clock.instant(), Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p2", 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p3", 5L)).futureValue
      tick()
      val offset2 = TimestampOffset(clock.instant(), Map("p1" -> 4L, "p3" -> 6L, "p4" -> 9L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p1", 4L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p3", 6L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset2, "p4", 9L)).futureValue
      tick()
      val offset3 = TimestampOffset(clock.instant(), Map("p5" -> 10L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset3, "p5", 10L)).futureValue

      def env(pid: Pid, seqNr: SeqNr, timestamp: Instant): EventEnvelope[String] =
        createEnvelope(pid, seqNr, timestamp, "evt")

      offsetStore.validate(env("p5", 10, offset3.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p1", 4, offset2.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p3", 6, offset2.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p4", 9, offset2.timestamp)).futureValue shouldBe Duplicate

      offsetStore.validate(env("p1", 3, offset1.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p2", 1, offset1.timestamp)).futureValue shouldBe Duplicate
      offsetStore.validate(env("p3", 5, offset1.timestamp)).futureValue shouldBe Duplicate

      offsetStore.validate(env("p1", 2, offset1.timestamp.minusMillis(1))).futureValue shouldBe Duplicate
      offsetStore.validate(env("p5", 9, offset3.timestamp.minusMillis(1))).futureValue shouldBe Duplicate

      offsetStore.validate(env("p5", 11, offset3.timestamp)).futureValue shouldNot be(Duplicate)
      offsetStore.validate(env("p5", 12, offset3.timestamp.plusMillis(1))).futureValue shouldNot be(Duplicate)

      offsetStore.validate(env("p6", 1, offset3.timestamp.plusMillis(2))).futureValue shouldNot be(Duplicate)
      offsetStore.validate(env("p7", 1, offset3.timestamp.minusMillis(1))).futureValue shouldNot be(Duplicate)
    }

    "accept known sequence numbers and reject unknown" in {
      val projectionId = genRandomProjectionId()
      val eventTimestampQueryClock = TestClock.nowMicros()
      val offsetStore = createOffsetStore(projectionId, eventTimestampQueryClock = eventTimestampQueryClock)

      val startTime = TestClock.nowMicros().instant()
      val offset1 = TimestampOffset(startTime, Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p2", 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p3", 5L)).futureValue

      // seqNr 1 is always accepted
      val env1 = createEnvelope("p4", 1L, startTime.plusMillis(1), "e4-1")
      offsetStore.validate(env1).futureValue shouldBe Accepted
      offsetStore.validate(backtrackingEnvelope(env1)).futureValue shouldBe Accepted
      // but not if already inflight, seqNr 1 was accepted
      offsetStore.addInflight(env1)
      val env1Later = createEnvelope("p4", 1L, startTime.plusMillis(1), "e4-1")
      offsetStore.validate(env1Later).futureValue shouldBe Duplicate
      offsetStore.validate(backtrackingEnvelope(env1Later)).futureValue shouldBe Duplicate
      // subsequent seqNr is accepted
      val env2 = createEnvelope("p4", 2L, startTime.plusMillis(2), "e4-2")
      offsetStore.validate(env2).futureValue shouldBe Accepted
      offsetStore.validate(backtrackingEnvelope(env2)).futureValue shouldBe Accepted
      offsetStore.addInflight(env2)
      // but not when gap
      val envP4SeqNr4 = createEnvelope("p4", 4L, startTime.plusMillis(3), "e4-4")
      offsetStore.validate(envP4SeqNr4).futureValue shouldBe RejectedSeqNr
      // hard reject when gap from backtracking
      offsetStore.validate(backtrackingEnvelope(envP4SeqNr4)).futureValue shouldBe RejectedBacktrackingSeqNr
      // reject filtered event when gap
      offsetStore.validate(filteredEnvelope(envP4SeqNr4)).futureValue shouldBe RejectedSeqNr
      // hard reject when filtered event with gap from backtracking
      offsetStore
        .validate(backtrackingEnvelope(filteredEnvelope(envP4SeqNr4)))
        .futureValue shouldBe RejectedBacktrackingSeqNr
      // and not if later already inflight, seqNr 2 was accepted
      offsetStore.validate(createEnvelope("p4", 1L, startTime.plusMillis(1), "e4-1")).futureValue shouldBe Duplicate

      // +1 to known is accepted
      val env3 = createEnvelope("p1", 4L, startTime.plusMillis(4), "e1-4")
      offsetStore.validate(env3).futureValue shouldBe Accepted
      // but not same
      offsetStore.validate(createEnvelope("p3", 5L, startTime, "e3-5")).futureValue shouldBe Duplicate
      // but not same, even if it's 1
      offsetStore.validate(createEnvelope("p2", 1L, startTime, "e2-1")).futureValue shouldBe Duplicate
      // and not less
      offsetStore.validate(createEnvelope("p3", 4L, startTime, "e3-4")).futureValue shouldBe Duplicate
      offsetStore.addInflight(env3)
      // and then it's not accepted again
      offsetStore.validate(env3).futureValue shouldBe Duplicate
      offsetStore.validate(backtrackingEnvelope(env3)).futureValue shouldBe Duplicate
      // and not when later seqNr is inflight
      offsetStore.validate(env2).futureValue shouldBe Duplicate
      offsetStore.validate(backtrackingEnvelope(env2)).futureValue shouldBe Duplicate

      // +1 to known, and then also subsequent are accepted (needed for grouped)
      val env4 = createEnvelope("p3", 6L, startTime.plusMillis(5), "e3-6")
      offsetStore.validate(env4).futureValue shouldBe Accepted
      offsetStore.addInflight(env4)
      val env5 = createEnvelope("p3", 7L, startTime.plusMillis(6), "e3-7")
      offsetStore.validate(env5).futureValue shouldBe Accepted
      offsetStore.addInflight(env5)
      val env6 = createEnvelope("p3", 8L, startTime.plusMillis(7), "e3-8")
      offsetStore.validate(env6).futureValue shouldBe Accepted
      offsetStore.addInflight(env6)

      // reject unknown
      val env7 = createEnvelope("p5", 7L, startTime.plusMillis(8), "e5-7")
      offsetStore.validate(env7).futureValue shouldBe RejectedSeqNr
      offsetStore.validate(backtrackingEnvelope(env7)).futureValue shouldBe RejectedBacktrackingSeqNr
      // but ok when previous is old
      eventTimestampQueryClock.setInstant(startTime.minusSeconds(3600))
      val env8 = createEnvelope("p5", 7L, startTime.plusMillis(5), "e5-7")
      offsetStore.validate(env8).futureValue shouldBe Accepted
      eventTimestampQueryClock.setInstant(startTime)
      offsetStore.addInflight(env8)
      // and subsequent seqNr is accepted
      val env9 = createEnvelope("p5", 8L, startTime.plusMillis(9), "e5-8")
      offsetStore.validate(env9).futureValue shouldBe Accepted
      offsetStore.addInflight(env9)

      // reject unknown filtered
      val env10 = filteredEnvelope(createEnvelope("p6", 7L, startTime.plusMillis(10), "e6-7"))
      offsetStore.validate(env10).futureValue shouldBe RejectedSeqNr
      // hard reject when unknown from backtracking
      offsetStore.validate(backtrackingEnvelope(env10)).futureValue shouldBe RejectedBacktrackingSeqNr
      // hard reject when unknown filtered event from backtracking
      offsetStore
        .validate(backtrackingEnvelope(filteredEnvelope(env10)))
        .futureValue shouldBe RejectedBacktrackingSeqNr

      // it's keeping the inflight that are not in the "stored" state
      offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 8, "p4" -> 2L, "p5" -> 8)
      // and they are removed from inflight once they have been stored
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(2), Map("p4" -> 2L)), "p4", 2L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(9), Map("p5" -> 8L)), "p5", 8L))
        .futureValue
      offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 8)
    }

    "update inflight on error and re-accept element" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val startTime = TestClock.nowMicros().instant()

      val envelope1 = createEnvelope("p1", 1L, startTime.plusMillis(1), "e1-1")
      val envelope2 = createEnvelope("p1", 2L, startTime.plusMillis(2), "e1-2")
      val envelope3 = createEnvelope("p1", 3L, startTime.plusMillis(2), "e1-2")

      // seqNr 1 is always accepted
      offsetStore.validate(envelope1).futureValue shouldBe Accepted
      offsetStore.addInflight(envelope1)
      offsetStore.getInflight() shouldBe Map("p1" -> 1L)
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(1), Map("p1" -> 1L)), "p1", 1L))
        .futureValue
      offsetStore.getInflight() shouldBe empty

      // seqNr 2 is accepts since it follows seqNr 1 that is stored in state
      offsetStore.validate(envelope2).futureValue shouldBe Accepted
      // simulate envelope processing error by not adding envelope2 to inflight

      // seqNr 3 is not accepted, still waiting for seqNr 2
      offsetStore.validate(envelope3).futureValue shouldBe RejectedSeqNr

      // offer seqNr 2 once again
      offsetStore.validate(envelope2).futureValue shouldBe Accepted
      offsetStore.addInflight(envelope2)
      offsetStore.getInflight() shouldBe Map("p1" -> 2L)

      // offer seqNr 3  once more
      offsetStore.validate(envelope3).futureValue shouldBe Accepted
      offsetStore.addInflight(envelope3)
      offsetStore.getInflight() shouldBe Map("p1" -> 3L)

      // and they are removed from inflight once they have been stored
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(2), Map("p1" -> 3L)), "p1", 3L))
        .futureValue
      offsetStore.getInflight() shouldBe empty
    }

    "mapIsAccepted" in {
      val projectionId = genRandomProjectionId()
      val startTime = TestClock.nowMicros().instant()
      val offsetStore = createOffsetStore(projectionId)

      val offset1 = TimestampOffset(startTime, Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p2", 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p3", 5L)).futureValue

      // seqNr 1 is always accepted
      val env1 = createEnvelope("p4", 1L, startTime.plusMillis(1), "e4-1")
      // subsequent seqNr is accepted
      val env2 = createEnvelope("p4", 2L, startTime.plusMillis(2), "e4-2")
      // but not when gap
      val env3 = createEnvelope("p4", 4L, startTime.plusMillis(3), "e4-4")
      // ok when previous is known
      val env4 = createEnvelope("p1", 4L, startTime.plusMillis(5), "e1-4")
      // but not when previous is unknown
      val env5 = createEnvelope("p3", 7L, startTime.plusMillis(5), "e3-7")

      offsetStore.validateAll(List(env1, env2, env3, env4, env5)).futureValue shouldBe List(
        env1 -> Accepted,
        env2 -> Accepted,
        env3 -> RejectedSeqNr,
        env4 -> Accepted,
        env5 -> RejectedSeqNr)

    }

    "accept new revisions for durable state" in {
      val projectionId = genRandomProjectionId()
      val offsetStore = createOffsetStore(projectionId)

      val startTime = TestClock.nowMicros().instant()
      val offset1 = TimestampOffset(startTime, Map("p1" -> 3L, "p2" -> 1L, "p3" -> 5L))
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p1", 3L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p2", 1L)).futureValue
      offsetStore.saveOffset(OffsetPidSeqNr(offset1, "p3", 5L)).futureValue

      // seqNr 1 is always accepted
      val env1 = createUpdatedDurableState("p4", 1L, startTime.plusMillis(1), "s4-1")
      offsetStore.validate(env1).futureValue shouldBe Accepted
      // but not if already inflight, seqNr 1 was accepted
      offsetStore.addInflight(env1)
      offsetStore
        .validate(createUpdatedDurableState("p4", 1L, startTime.plusMillis(1), "s4-1"))
        .futureValue shouldBe Duplicate
      // subsequent seqNr is accepted
      val env2 = createUpdatedDurableState("p4", 2L, startTime.plusMillis(2), "s4-2")
      offsetStore.validate(env2).futureValue shouldBe Accepted
      offsetStore.addInflight(env2)
      // and also ok with gap
      offsetStore
        .validate(createUpdatedDurableState("p4", 4L, startTime.plusMillis(3), "s4-4"))
        .futureValue shouldBe Accepted
      // and not if later already inflight, seqNr 2 was accepted
      offsetStore
        .validate(createUpdatedDurableState("p4", 1L, startTime.plusMillis(1), "s4-1"))
        .futureValue shouldBe Duplicate

      // greater than known is accepted
      val env3 = createUpdatedDurableState("p1", 4L, startTime.plusMillis(4), "s1-4")
      offsetStore.validate(env3).futureValue shouldBe Accepted
      // but not same
      offsetStore.validate(createUpdatedDurableState("p3", 5L, startTime, "s3-5")).futureValue shouldBe Duplicate
      // but not same, even if it's 1
      offsetStore.validate(createUpdatedDurableState("p2", 1L, startTime, "s2-1")).futureValue shouldBe Duplicate
      // and not less
      offsetStore.validate(createUpdatedDurableState("p3", 4L, startTime, "s3-4")).futureValue shouldBe Duplicate
      offsetStore.addInflight(env3)

      // greater than known, and then also subsequent are accepted (needed for grouped)
      val env4 = createUpdatedDurableState("p3", 8L, startTime.plusMillis(5), "s3-6")
      offsetStore.validate(env4).futureValue shouldBe Accepted
      offsetStore.addInflight(env4)
      val env5 = createUpdatedDurableState("p3", 9L, startTime.plusMillis(6), "s3-7")
      offsetStore.validate(env5).futureValue shouldBe Accepted
      offsetStore.addInflight(env5)
      val env6 = createUpdatedDurableState("p3", 20L, startTime.plusMillis(7), "s3-8")
      offsetStore.validate(env6).futureValue shouldBe Accepted
      offsetStore.addInflight(env6)

      // accept unknown
      val env7 = createUpdatedDurableState("p5", 7L, startTime.plusMillis(8), "s5-7")
      offsetStore.validate(env7).futureValue shouldBe Accepted
      offsetStore.addInflight(env7)

      // it's keeping the inflight that are not in the "stored" state
      offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 20, "p4" -> 2L, "p5" -> 7)
      // and they are removed from inflight once they have been stored
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(2), Map("p4" -> 2L)), "p4", 2L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plusMillis(9), Map("p5" -> 8L)), "p5", 8L))
        .futureValue
      offsetStore.getInflight() shouldBe Map("p1" -> 4L, "p3" -> 20)
    }

    "evict old records from same slice" in {
      val projectionId = genRandomProjectionId()
      val evictSettings = settings.withTimeWindow(JDuration.ofSeconds(100)).withEvictInterval(JDuration.ofSeconds(10))
      import evictSettings._
      val offsetStore = createOffsetStore(projectionId, evictSettings)

      val startTime = TestClock.nowMicros().instant()
      log.debug("Start time [{}]", startTime)

      // these pids have the same slice 645
      val p1 = "p500"
      val p2 = "p621"
      val p3 = "p742"
      val p4 = "p863"
      val p5 = "p984"
      val p6 = "p3080"
      val p7 = "p4290"
      val p8 = "p20180"

      offsetStore.saveOffset(OffsetPidSeqNr(TimestampOffset(startTime, Map(p1 -> 1L)), p1, 1L)).futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(JDuration.ofSeconds(1)), Map(p2 -> 1L)), p2, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(JDuration.ofSeconds(2)), Map(p3 -> 1L)), p3, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(evictInterval), Map(p4 -> 1L)), p4, 1L))
        .futureValue
      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(1)), Map(p4 -> 1L)),
            p4,
            1L))
        .futureValue
      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(2)), Map(p5 -> 1L)),
            p5,
            1L))
        .futureValue
      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(3)), Map(p6 -> 1L)),
            p6,
            3L))
        .futureValue
      offsetStore.getState().size shouldBe 6

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.minusSeconds(10)), Map(p7 -> 1L)), p7, 1L))
        .futureValue
      offsetStore.getState().size shouldBe 7 // nothing evicted yet

      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).minusSeconds(3)), Map(p8 -> 1L)),
            p8,
            1L))
        .futureValue
      offsetStore.getState().size shouldBe 8 // still nothing evicted yet

      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).plusSeconds(1)), Map(p8 -> 2L)),
            p8,
            2L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p5, p6, p7, p8)

      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).plusSeconds(20)), Map(p8 -> 3L)),
            p8,
            3L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p7, p8)
    }

    "evict old records from different slices" in {
      val projectionId = genRandomProjectionId()
      val evictSettings = settings.withTimeWindow(JDuration.ofSeconds(100)).withEvictInterval(JDuration.ofSeconds(10))
      import evictSettings._
      val offsetStore = createOffsetStore(projectionId, evictSettings)

      val startTime = TestClock.nowMicros().instant()
      log.debug("Start time [{}]", startTime)

      val p1 = "p500" // slice 645
      val p2 = "p92" // slice 905
      val p3 = "p108" // slice 905
      val p4 = "p863" // slice 645
      val p5 = "p984" // slice 645
      val p6 = "p3080" // slice 645
      val p7 = "p4290" // slice 645
      val p8 = "p20180" // slice 645

      offsetStore.saveOffset(OffsetPidSeqNr(TimestampOffset(startTime, Map(p1 -> 1L)), p1, 1L)).futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(JDuration.ofSeconds(1)), Map(p2 -> 1L)), p2, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(JDuration.ofSeconds(2)), Map(p3 -> 1L)), p3, 1L))
        .futureValue
      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(evictInterval), Map(p4 -> 1L)), p4, 1L))
        .futureValue
      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(1)), Map(p4 -> 1L)),
            p4,
            1L))
        .futureValue
      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(2)), Map(p5 -> 1L)),
            p5,
            1L))
        .futureValue
      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(evictInterval).plus(JDuration.ofSeconds(3)), Map(p6 -> 1L)),
            p6,
            1L))
        .futureValue
      offsetStore.getState().size shouldBe 6

      offsetStore
        .saveOffset(OffsetPidSeqNr(TimestampOffset(startTime.plus(timeWindow.minusSeconds(10)), Map(p7 -> 1L)), p7, 1L))
        .futureValue
      offsetStore.getState().size shouldBe 7 // nothing evicted yet

      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).minusSeconds(3)), Map(p8 -> 1L)),
            p8,
            1L))
        .futureValue
      offsetStore.getState().size shouldBe 8 // still nothing evicted yet

      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).plusSeconds(1)), Map(p8 -> 2L)),
            p8,
            2L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p5, p6, p7, p8)

      offsetStore
        .saveOffset(
          OffsetPidSeqNr(
            TimestampOffset(startTime.plus(timeWindow.plus(evictInterval).plusSeconds(20)), Map(p8 -> 3L)),
            p8,
            3L))
        .futureValue
      offsetStore.getState().byPid.keySet shouldBe Set(p7, p8)
    }

    "start from earliest slice" in {
      // FIXME r2dbc has the condition " when projection key is changed"
      val projectionId1 = ProjectionId(UUID.randomUUID().toString, "512-767")
      val projectionId2 = ProjectionId(projectionId1.name, "768-1023")
      val projectionId3 = ProjectionId(projectionId1.name, "512-1023")
      val offsetStore1 = new DynamoDBOffsetStore(
        projectionId1,
        Some(new TestTimestampSourceProvider(512, 767, clock)),
        system,
        settings,
        client)
      val offsetStore2 = new DynamoDBOffsetStore(
        projectionId2,
        Some(new TestTimestampSourceProvider(768, 1023, clock)),
        system,
        settings,
        client)

      val p1 = "p500" // slice 645
      val p2 = "p863" // slice 645
      val p3 = "p11" // slice 656
      val p4 = "p92" // slice 905

      val time1 = TestClock.nowMicros().instant()
      val time2 = time1.plusSeconds(1)
      val time3 = time1.plusSeconds(2)
      val time4 = time1.plusSeconds(3 * 60) // far ahead

      offsetStore1.saveOffset(OffsetPidSeqNr(TimestampOffset(time1, Map(p1 -> 1L)), p1, 1L)).futureValue
      offsetStore1.saveOffset(OffsetPidSeqNr(TimestampOffset(time2, Map(p2 -> 1L)), p2, 1L)).futureValue
      offsetStore1.saveOffset(OffsetPidSeqNr(TimestampOffset(time3, Map(p3 -> 1L)), p3, 1L)).futureValue
      offsetStore2
        .saveOffset(OffsetPidSeqNr(TimestampOffset(time4, Map(p4 -> 1L)), p4, 1L))
        .futureValue

      // after downscaling
      val offsetStore3 = new DynamoDBOffsetStore(
        projectionId3,
        Some(new TestTimestampSourceProvider(512, 1023, clock)),
        system,
        settings,
        client)

      val offset =
        TimestampOffset.toTimestampOffset(offsetStore3.readOffset().futureValue.get) // this will load from database
      offsetStore3.getState().offsetBySlice.size shouldBe 3

      offset.timestamp shouldBe time2
      // FIXME seen not reconstructed
//      offset.seen shouldBe Map(p2 -> 1L)

      // getOffset is used by management api, and that should not be adjusted
      // FIXME TimestampOffset.toTimestampOffset(offsetStore3.getOffset().futureValue.get).timestamp shouldBe time4
    }

    // FIXME more tests, see r2dbc
    //    "set offset" in {
    //    "clear offset" in {
    //    "read and save paused" in {

  }
}
