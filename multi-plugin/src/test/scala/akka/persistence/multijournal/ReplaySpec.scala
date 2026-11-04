/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import java.time.Instant

import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.testkit.TestProbe
import akka.persistence.JournalProtocol
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

object ReplaySpec {
  val Journals = ConfigFactory.empty
    .withFallback(TestProbeJournal.config("replay-spec-cold-journal", "cold-journal"))
    .withFallback(TestProbeJournal.config("replay-spec-lukewarm-journal", "lukewarm-journal"))
    .withFallback(TestProbeJournal.config("replay-spec-hot-journal", "hot-journal"))

  val OneJournalConfig = ConfigFactory
    .parseString("""
    |akka.persistence.multi-journal {
    |  journals {
    |    0 = "cold-journal"
    |  }
    |}
    """.stripMargin)
    .withFallback(Journals)
    .withFallback(ConfigFactory.load())

  val ShiftedOneJournalConfig = ConfigFactory
    .parseString("""
    |akka.persistence.multi-journal.journals {
    |  1 = "cold-journal"
    |}
    """.stripMargin)
    .withFallback(Journals)
    .withFallback(ConfigFactory.load())

  val TwoJournalsConfig = ConfigFactory
    .parseString("""
    |akka.persistence.multi-journal {
    |  journals {
    |    0 = "cold-journal"
    |    1 = "lukewarm-journal"
    |  }
    |}""".stripMargin)
    .withFallback(Journals)
    .withFallback(ConfigFactory.load())

  val ThreeJournalConfig = ConfigFactory
    .parseString("""
    |akka.persistence.multi-journal {
    |  journals {
    |    0 = "cold-journal"
    |    1 = "lukewarm-journal"
    |    2 = "hot-journal"
    |  }
    |}""".stripMargin)
    .withFallback(Journals)
    .withFallback(ConfigFactory.load())

  class Setup(td: TestData)(implicit classicSystem: akka.actor.ActorSystem) {
    val pid = td.nextPid()
    val actorProbe = TestProbe()

    def replayMessagesReq(fromSeqNr: Long, toSeqNr: Long, max: Long) =
      JournalProtocol.ReplayMessages(fromSeqNr, toSeqNr, max, pid, actorProbe.ref)

    def event(seqNr: Long, time: Instant, metadata: Option[Any]): PersistentRepr =
      metadata.fold(baseEvent(seqNr, time))(m => baseEvent(seqNr, time).withMetadata(m))

    private def baseEvent(seqNr: Long, time: Instant): PersistentRepr =
      PersistentRepr(s"e-$seqNr", seqNr, pid).withTimestamp(time.toEpochMilli)
  }
}

abstract class SingleJournalReplaySpecBase(config: Config, journalId: Int)
    extends ScalaTestWithActorTestKit(config)
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with LogCapturing
    with TestData {
  import ProbeOps._
  import ReplaySpec._

  implicit val classicSystem: akka.actor.ActorSystem = system.classicSystem
  val journal = Persistence(system).journalFor("akka.persistence.multi-journal")
  val coldJournalProbe = TestProbeJournal.getProbe("replay-spec-cold-journal")

  def getMetadata(pid: String): Future[Map[Int, Long]] = {
    val promise = Promise[Map[Int, Long]]()
    journal ! MultiJournal.GetMetadata(pid, promise)
    promise.future
  }

  def metadataForEarlierEvents(highestSequenceNr: Long): Option[MultiJournalMetadata] =
    if (journalId == 0) None else Some(MultiJournalMetadata(Map(0 -> highestSequenceNr)))

  def metadataAsMap(highestSequenceNr: Long): Map[Int, Long] =
    metadataForEarlierEvents(highestSequenceNr).fold(Map.empty[Int, Long])(_.toMap)

  override def beforeAll(): Unit = {
    super.beforeAll()
    getMetadata(nextPid()).futureValue shouldBe Map.empty
  }

  override def afterEach(): Unit = {
    super.afterEach()
    coldJournalProbe.expectNoMessage(10.millis)
  }

  // Journal 0 cannot have legitimate metadata; the shifted journal uses metadata to locate earlier events in journal 0.
  (if (journalId == 0) "MultiJournal (one journal)" else "MultiJournal (one shifted journal)") should {
    "report failed replays" in new Setup(this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.failure(new RuntimeException("journal go boom"))
      }
      actorProbe.expectMsgType[JournalProtocol.ReplayMessagesFailure].cause.getMessage shouldBe "journal go boom"
      getMetadata(pid).futureValue shouldBe Map.empty

      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(6)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.pid shouldBe pid
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 6
        rm.promise.failure(new RuntimeException("journal go boom (replaying)"))
      }
      actorProbe
        .expectMsgType[JournalProtocol.ReplayMessagesFailure]
        .cause
        .getMessage shouldBe "journal go boom (replaying)"
      getMetadata(pid).futureValue shouldBe Map.empty
    }

    "proxy replay (no snapshot) [no events in journal]" in new Setup(this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(0)
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 0
      getMetadata(pid).futureValue shouldBe Map.empty
    }

    "proxy replay (no snapshot) [events in journal]" in new Setup(this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(10)
      }
      val eventsFromColdJournal = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.pid shouldBe pid
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 10
        rm.max shouldBe Long.MaxValue
        val events = (1 to 10).map(event(_, Instant.now(), None))
        rm.promise.success(events)
        events
      }

      (1 to 10).map { seqNr =>
        val repr = actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent
        repr.metadata shouldBe empty
        repr shouldBe eventsFromColdJournal(seqNr - 1)
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 10
      getMetadata(pid).futureValue shouldBe Map.empty
    }

    "proxy replay (no snapshot) [deleted events in journal]" in new Setup(this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(11)
      }
      val eventsFromColdJournal = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.pid shouldBe pid
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 11
        rm.max shouldBe Long.MaxValue
        val events = (6 to 11).map(event(_, Instant.now(), metadataForEarlierEvents(5)))
        rm.promise.success(events)
        events
      }

      (6 to 11).foreach { seqNr =>
        val repr = actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent
        repr.metadata shouldBe empty
        repr shouldBe eventsFromColdJournal(seqNr - 6)
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 11
      getMetadata(pid).futureValue shouldBe metadataAsMap(5)
    }

    "proxy replay (from snapshot) [no events since snapshot]" in new Setup(this) {
      val snapshotSeqNr = 42L
      journal ! replayMessagesReq(snapshotSeqNr + 1, Long.MaxValue, Long.MaxValue)
      val metadataFut = getMetadata(pid)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 42
        rhsn.promise.success(42)
      }
      // multi-plugin declares success and checks the last event for metadata, which looks to the proxied journal
      // like another replay
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 42
      akka.pattern.after(30.millis)(metadataFut).value shouldBe None
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(42) // another test checks the behavior if this changes under us
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.pid shouldBe pid
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 42
        rm.max shouldBe 1
        rm.promise.success(Seq(event(42, Instant.now(), None)))
      }
      metadataFut.futureValue shouldBe Map.empty
    }

    "proxy replay (from snapshot) [some events after snapshot are deleted/expired]" in new Setup(this) {
      val snapshotSeqNr = 420L
      journal ! replayMessagesReq(snapshotSeqNr + 1, Long.MaxValue, Long.MaxValue)
      val metadataFut = getMetadata(pid)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 420
        rhsn.promise.success(567)
      }
      val eventsFromColdJournal = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.pid shouldBe pid
        rm.fromSequenceNr shouldBe 421
        rm.toSequenceNr shouldBe 567
        rm.max shouldBe Long.MaxValue
        val events = (499 to 567).map(event(_, Instant.now(), metadataForEarlierEvents(460)))
        rm.promise.success(events)
        events
      }

      (499 to 567).foreach { seqNr =>
        val repr = actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent
        repr.metadata shouldBe empty
        repr shouldBe eventsFromColdJournal(seqNr - 499)
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 567
      metadataFut.futureValue shouldBe metadataAsMap(460)
    }

    "proxy bounded replays" in new Setup(this) {
      journal ! replayMessagesReq(50, 550, 501)
      val metadataFut = getMetadata(pid)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.promise.success(1000)
      }
      val eventsFromColdJournal = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.pid shouldBe pid
        rm.fromSequenceNr shouldBe 50
        rm.toSequenceNr shouldBe 550
        rm.max shouldBe 501
        val events = (50 to 550).map(event(_, Instant.now(), metadataForEarlierEvents(49)))
        rm.promise.success(events)
        events
      }

      (50 to 550).foreach { seqNr =>
        val repr = actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent
        repr.metadata shouldBe empty
        repr shouldBe eventsFromColdJournal(seqNr - 50)
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 1000
      akka.pattern.after(30.millis)(metadataFut).value shouldBe None
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.promise.success(1000)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe -1
        rm.promise.success(Seq(event(1000, Instant.now(), metadataForEarlierEvents(49))))
      }
      metadataFut.futureValue shouldBe metadataAsMap(49)
    }

    "support the read-highest-seq-nr query" in new Setup(this) {
      journal ! replayMessagesReq(1, 0, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(20)
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 20
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(20)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 20
        rm.promise.success(Nil)
      }
    }

    "support the replay-only-last query" in new Setup(this) {
      journal ! replayMessagesReq(-1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(27)
      }
      val evt = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 27
        val genEvent = event(27, Instant.now(), metadataForEarlierEvents(13))
        rm.promise.success(Seq(genEvent))
        genEvent
      }
      actorProbe.asserting[JournalProtocol.ReplayedMessage] { rm =>
        rm.persistent.metadata shouldBe empty
        rm.persistent shouldBe evt
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 27
      getMetadata(pid).futureValue shouldBe metadataAsMap(13)
    }

    "detect competing writes when checking for metadata" in new Setup(this) {
      journal ! replayMessagesReq(1, 10, 10)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(30)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.toSequenceNr shouldBe 10
        rm.promise.success((1 to 10).map(event(_, Instant.now(), None)))
      }
      actorProbe.receiveN(11).iterator.zipWithIndex.foreach { case (msg, i) =>
        if (i < 10) {
          msg shouldBe a[JournalProtocol.ReplayedMessage]
          msg.asInstanceOf[JournalProtocol.ReplayedMessage].persistent.sequenceNr shouldBe (i + 1)
        } else {
          msg shouldBe a[JournalProtocol.RecoverySuccess]
          msg.asInstanceOf[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 30
        }
      }
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(32)
      }
      LoggingTestKit
        .error(s"Detected competing write for [$pid], stopping per-pid actor (expected=30, actual=32)")
        .expect {
          coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
            rm.fromSequenceNr shouldBe -1
            rm.toSequenceNr shouldBe 32
            rm.promise.success(Seq(event(32, Instant.now(), None)))
          }
        }
    }

    "serve replays after a competing write from a new per-pid actor" in new Setup(this) {
      journal ! replayMessagesReq(1, 10, 10)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(30)
      coldJournalProbe
        .expectMessageType[TestProbeJournal.ReplayMessages]
        .promise
        .success((1 to 10).map(event(_, Instant.now(), None)))
      actorProbe.receiveN(10).foreach { _ shouldBe a[JournalProtocol.ReplayedMessage] }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 30
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(32)
      LoggingTestKit.error(s"Detected competing write for [$pid]").expect {
        coldJournalProbe
          .expectMessageType[TestProbeJournal.ReplayMessages]
          .promise
          .success(Seq(event(32, Instant.now(), None)))
      }

      // the per-pid actor which detected the competing write must not handle this
      val otherProbe = TestProbe()
      journal ! replayMessagesReq(31, Long.MaxValue, Long.MaxValue).copy(persistentActor = otherProbe.ref)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 30
        rhsn.promise.success(32)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 31
        rm.toSequenceNr shouldBe 32
        rm.promise.success((31 to 32).map(event(_, Instant.now(), None)))
      }
      (31 to 32).foreach { seqNr =>
        otherProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent.sequenceNr shouldBe seqNr
      }
      otherProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 32
    }

    "hand off a replay queued behind a competing write to a new per-pid actor" in new Setup(this) {
      journal ! replayMessagesReq(1, 10, 10)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(30)
      coldJournalProbe
        .expectMessageType[TestProbeJournal.ReplayMessages]
        .promise
        .success((1 to 10).map(event(_, Instant.now(), None)))
      actorProbe.receiveN(10).foreach { _ shouldBe a[JournalProtocol.ReplayedMessage] }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 30
      val probe = coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr]

      // queued behind the metadata probe
      val otherProbe = TestProbe()
      journal ! replayMessagesReq(31, Long.MaxValue, Long.MaxValue).copy(persistentActor = otherProbe.ref)
      otherProbe.expectNoMessage(100.millis)

      probe.promise.success(32)
      LoggingTestKit.error(s"Detected competing write for [$pid]").expect {
        coldJournalProbe
          .expectMessageType[TestProbeJournal.ReplayMessages]
          .promise
          .success(Seq(event(32, Instant.now(), None)))
      }

      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 30
        rhsn.promise.success(32)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 31
        rm.toSequenceNr shouldBe 32
        rm.promise.success((31 to 32).map(event(_, Instant.now(), None)))
      }
      (31 to 32).foreach { seqNr =>
        otherProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent.sequenceNr shouldBe seqNr
      }
      otherProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 32
    }

    "delay simultaneous recoveries for the same pid" in new Setup(this) {
      journal ! replayMessagesReq(1, 10, 10)
      val otherProbe = TestProbe()
      journal ! replayMessagesReq(-1, Long.MaxValue, Long.MaxValue).copy(persistentActor = otherProbe.ref)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(100)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 10
        rm.promise.success((1 to 10).map(event(_, Instant.now(), None)))
      }
      actorProbe.receiveN(10).foreach { _ shouldBe a[JournalProtocol.ReplayedMessage] }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 100

      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(100)
      }
      val lastEvent = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 100
        val evt = event(100, Instant.now(), None)
        rm.promise.success(Seq(evt))
        evt
      }
      otherProbe.expectNoMessage(10.millis)

      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(100)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 100
        rm.promise.success(Seq(lastEvent))
      }
      actorProbe.expectNoMessage(10.millis)
      otherProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent.sequenceNr shouldBe 100
      otherProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 100
    }

    "strip future metadata from the events" in new Setup(this) {
      journal ! replayMessagesReq(4, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)

      val events = ((4 to 14).iterator.map(event(_, Instant.now(), None)) ++ (Some(
        event(15, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 3, 1 -> 30))))))).toSeq
      coldJournalProbe.expectMessageType[TestProbeJournal.ReplayMessages].promise.success(events)

      actorProbe.receiveN(11).iterator.zip(events.iterator.take(14)).foreach { case (msg, event) =>
        msg shouldBe a[JournalProtocol.ReplayedMessage]
        msg.asInstanceOf[JournalProtocol.ReplayedMessage].persistent shouldBe event
      }
      actorProbe.asserting[JournalProtocol.ReplayedMessage] { rm =>
        rm.persistent.sequenceNr shouldBe 15
        rm.persistent.metadata shouldBe empty
      }

      journalId match {
        case 0 => getMetadata(pid).futureValue shouldBe Map.empty
        case 1 => getMetadata(pid).futureValue shouldBe Map(0 -> 3L)
      }
    }
  }
}

class SingleJournalReplaySpec extends SingleJournalReplaySpecBase(ReplaySpec.OneJournalConfig, 0)

class ShiftedSingleJournalReplaySpec extends SingleJournalReplaySpecBase(ReplaySpec.ShiftedOneJournalConfig, 1)

class TwoJournalReplaySpec
    extends ScalaTestWithActorTestKit(ReplaySpec.TwoJournalsConfig)
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with LogCapturing
    with TestData {
  import ProbeOps._
  import ReplaySpec._

  implicit val classicSystem: akka.actor.ActorSystem = system.classicSystem
  val journal = Persistence(system).journalFor("akka.persistence.multi-journal")
  val coldJournalProbe = TestProbeJournal.getProbe("replay-spec-cold-journal")
  val lukewarmJournalProbe = TestProbeJournal.getProbe("replay-spec-lukewarm-journal")

  def getMetadata(pid: String): Future[Map[Int, Long]] = {
    val promise = Promise[Map[Int, Long]]()
    journal ! MultiJournal.GetMetadata(pid, promise)
    promise.future
  }

  def startReplay(setup: Setup, fromSeqNr: Long, toSeqNr: Long, max: Long): Unit = {
    import setup._
    journal ! replayMessagesReq(fromSeqNr, toSeqNr, max)
    lukewarmJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
      rhsn.pid shouldBe pid
      rhsn.fromSequenceNr shouldBe math.max(0L, fromSeqNr - 1)
      rhsn.promise.success(0)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    getMetadata(nextPid()).futureValue shouldBe Map.empty
  }

  override def afterEach(): Unit = {
    super.afterEach()
    coldJournalProbe.expectNoMessage(10.millis)
    lukewarmJournalProbe.expectNoMessage(10.millis)
  }

  "MultiJournal (two journals, events only in cold journal)" should {
    "report failed replays" in new Setup(this) {
      startReplay(this, 1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.failure(new RuntimeException("journal go boom"))
      }
      actorProbe.expectMsgType[JournalProtocol.ReplayMessagesFailure].cause.getMessage shouldBe "journal go boom"
      getMetadata(pid).futureValue shouldBe Map.empty

      startReplay(this, 1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(6)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.pid shouldBe pid
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 6
        rm.promise.failure(new RuntimeException("journal go boom (replaying)"))
      }
      actorProbe
        .expectMsgType[JournalProtocol.ReplayMessagesFailure]
        .cause
        .getMessage shouldBe "journal go boom (replaying)"
      getMetadata(pid).futureValue shouldBe Map.empty
    }

    "proxy replay (no snapshot) [no events in either journal]" in new Setup(this) {
      startReplay(this, 1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.pid shouldBe pid
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(0)
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 0
      getMetadata(pid).futureValue shouldBe Map.empty
    }

    "proxy replay (no snapshot) [events in cold journal]" in new Setup(this) {
      startReplay(this, 1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      val events = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.pid shouldBe pid
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 10
        rm.max shouldBe Long.MaxValue
        val events = (1 to 10).map(event(_, Instant.now(), None))
        rm.promise.success(events)
        events
      }
      events.foreach { expected =>
        actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent shouldBe expected
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 10
      getMetadata(pid).futureValue shouldBe Map(0 -> 10L)
    }

    "proxy replay (no snapshot) [deleted events in cold journal]" in new Setup(this) {
      startReplay(this, 1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(11)
      val events = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 11
        val events = (6 to 11).map(event(_, Instant.now(), None))
        rm.promise.success(events)
        events
      }
      events.foreach { expected =>
        actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent shouldBe expected
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 11
      getMetadata(pid).futureValue shouldBe Map(0 -> 11L)
    }

    "proxy replay (from snapshot) [no events since snapshot]" in new Setup(this) {
      val snapshotSeqNr = 42L
      startReplay(this, snapshotSeqNr + 1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe snapshotSeqNr
        rhsn.promise.success(snapshotSeqNr)
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe snapshotSeqNr
      getMetadata(pid).futureValue shouldBe Map.empty
    }

    "proxy replay (from snapshot) [some cold events after snapshot are deleted/expired]" in new Setup(this) {
      val snapshotSeqNr = 420L
      startReplay(this, snapshotSeqNr + 1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(567)
      val events = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.fromSequenceNr shouldBe 421
        rm.toSequenceNr shouldBe 567
        val events = (499 to 567).map(event(_, Instant.now(), None))
        rm.promise.success(events)
        events
      }
      events.foreach { expected =>
        actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent shouldBe expected
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 567
      getMetadata(pid).futureValue shouldBe Map(0 -> 567L)
    }

    "proxy bounded replays" in new Setup(this) {
      startReplay(this, 50, 550, 501)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(1000)
      val events = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.fromSequenceNr shouldBe 50
        rm.toSequenceNr shouldBe 550
        rm.max shouldBe 501
        val events = (50 to 550).map(event(_, Instant.now(), None))
        rm.promise.success(events)
        events
      }
      events.foreach { expected =>
        actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent shouldBe expected
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 1000
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(1000)
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 1000
        rm.promise.success(Seq(event(1000, Instant.now(), None)))
      }
      getMetadata(pid).futureValue shouldBe Map(0 -> 1000L)
    }

    "support the read-highest-seq-nr query" in new Setup(this) {
      startReplay(this, 1, 0, Long.MaxValue)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(20)
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 20
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(20)
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 20
        rm.promise.success(Nil)
      }
      getMetadata(pid).futureValue shouldBe Map(0 -> 20L)
    }

    "support the replay-only-last query" in new Setup(this) {
      startReplay(this, -1, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(27)
      val evt = coldJournalProbe.asserting { (rm: TestProbeJournal.ReplayMessages) =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 27
        val evt = event(27, Instant.now(), None)
        rm.promise.success(Seq(evt))
        evt
      }
      actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent shouldBe evt
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 27
      getMetadata(pid).futureValue shouldBe Map(0 -> 27L)
    }

    "detect competing writes when checking for metadata" in new Setup(this) {
      startReplay(this, 1, 10, 10)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(30)
      coldJournalProbe
        .expectMessageType[TestProbeJournal.ReplayMessages]
        .promise
        .success((1 to 10).map(event(_, Instant.now(), None)))
      actorProbe.receiveN(10).foreach { _ shouldBe a[JournalProtocol.ReplayedMessage] }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 30
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(32)
      LoggingTestKit.error(s"Detected competing write for [$pid], stopping per-pid actor").expect {
        coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
          rm.fromSequenceNr shouldBe -1
          rm.toSequenceNr shouldBe 32
          rm.promise.success(Seq(event(32, Instant.now(), None)))
        }
      }
    }

    "delay simultaneous recoveries for the same pid" in new Setup(this) {
      journal ! replayMessagesReq(1, 10, 10)
      val otherProbe = TestProbe()
      journal ! replayMessagesReq(-1, Long.MaxValue, Long.MaxValue).copy(persistentActor = otherProbe.ref)

      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(100)
      coldJournalProbe
        .expectMessageType[TestProbeJournal.ReplayMessages]
        .promise
        .success((1 to 10).map(event(_, Instant.now(), None)))
      actorProbe.receiveN(10).foreach { _ shouldBe a[JournalProtocol.ReplayedMessage] }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 100
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(100)
      val lastEvent = event(100, Instant.now(), None)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReplayMessages].promise.success(Seq(lastEvent))

      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(100)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReplayMessages].promise.success(Seq(lastEvent))
      otherProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent shouldBe lastEvent
      otherProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 100
    }

    "replay from a sequence number above 1" in new Setup(this) {
      startReplay(this, 4, Long.MaxValue, Long.MaxValue)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
      val events = (4 to 15).map(event(_, Instant.now(), None))
      coldJournalProbe.expectMessageType[TestProbeJournal.ReplayMessages].promise.success(events)
      events.foreach { expected =>
        actorProbe.expectMsgType[JournalProtocol.ReplayedMessage].persistent shouldBe expected
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 15
      getMetadata(pid).futureValue shouldBe Map(0 -> 15L)
    }
  }

  "MultiJournal (history shifts from cold to lukewarm journal)" should {
    "replay across the journal boundary" in new Setup(this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 10
        rm.max shouldBe Long.MaxValue
        rm.promise.success((6 to 10).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }

      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(5)
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 5
        rm.max shouldBe Long.MaxValue
        rm.promise.success((1 to 5).map(event(_, Instant.now(), None)))
      }

      (1 to 10).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 10
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L)
    }

    "replay from a snapshot before the journal boundary" in new Setup(this) {
      journal ! replayMessagesReq(4, Long.MaxValue, Long.MaxValue)
      lukewarmJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 3
        rhsn.promise.success(10)
      }
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 4
        rm.toSequenceNr shouldBe 10
        rm.promise.success((6 to 10).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }

      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 3
        rhsn.promise.success(5)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 4
        rm.toSequenceNr shouldBe 5
        rm.max shouldBe Long.MaxValue
        rm.promise.success((4 to 5).map(event(_, Instant.now(), None)))
      }

      (4 to 10).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 10
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L)
    }

    "avoid the cold journal when recovery starts after the journal boundary" in new Setup(this) {
      journal ! replayMessagesReq(8, Long.MaxValue, Long.MaxValue)
      lukewarmJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 7
        rhsn.promise.success(10)
      }
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 8
        rm.toSequenceNr shouldBe 10
        rm.promise.success((8 to 10).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }

      (8 to 10).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 10
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L)
    }

    "support bounded replays across the journal boundary" in new Setup(this) {
      journal ! replayMessagesReq(3, 8, 6)
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 3
        rm.toSequenceNr shouldBe 8
        rm.max shouldBe 6
        rm.promise.success((6 to 8).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 10
        rm.max shouldBe 1
        rm.promise.success(Seq(event(10, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }

      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(5)
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 3
        rm.toSequenceNr shouldBe 5
        rm.max shouldBe 6
        rm.promise.success((3 to 5).map(event(_, Instant.now(), None)))
      }

      (3 to 8).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 10
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L)
    }

    "support replay-only-last from the lukewarm journal" in new Setup(this) {
      journal ! replayMessagesReq(-1, Long.MaxValue, Long.MaxValue)
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      val lastEvent = event(10, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReplayMessages].promise.success(Seq(lastEvent))

      actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
        replayed.persistent.sequenceNr shouldBe 10
        replayed.persistent.metadata shouldBe empty
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 10
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L)
    }

    "replay lukewarm events when the start of history is deleted/expired and the cold journal has no events" in new Setup(
      this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(5)
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 5
        // events 1 and 2 were deleted/expired
        rm.promise.success((3 to 5).map(event(_, Instant.now(), Some(MultiJournalMetadata.Empty))))
      }

      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 0
        rhsn.promise.success(0)
      }

      (3 to 5).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 5
      getMetadata(pid).futureValue shouldBe Map.empty
    }
  }
}

class ThreeJournalReplaySpec
    extends ScalaTestWithActorTestKit(ReplaySpec.ThreeJournalConfig)
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with LogCapturing
    with TestData {
  import ProbeOps._
  import ReplaySpec._

  implicit val classicSystem: akka.actor.ActorSystem = system.classicSystem
  val journal = Persistence(system).journalFor("akka.persistence.multi-journal")
  val coldJournalProbe = TestProbeJournal.getProbe("replay-spec-cold-journal")
  val lukewarmJournalProbe = TestProbeJournal.getProbe("replay-spec-lukewarm-journal")
  val hotJournalProbe = TestProbeJournal.getProbe("replay-spec-hot-journal")

  def getMetadata(pid: String): Future[Map[Int, Long]] = {
    val promise = Promise[Map[Int, Long]]()
    journal ! MultiJournal.GetMetadata(pid, promise)
    promise.future
  }

  override def afterEach(): Unit = {
    super.afterEach()
    coldJournalProbe.expectNoMessage(10.millis)
    lukewarmJournalProbe.expectNoMessage(10.millis)
    hotJournalProbe.expectNoMessage(10.millis)
  }

  "MultiJournal (history shifts across three journals)" should {
    "replay across both journal boundaries" in new Setup(this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 15
        rm.max shouldBe Long.MaxValue
        rm.promise.success((11 to 15).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5, 1 -> 10))))))
      }

      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 6
        rm.toSequenceNr shouldBe 10
        rm.max shouldBe Long.MaxValue
        rm.promise.success((6 to 10).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }

      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(5)
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 5
        rm.max shouldBe Long.MaxValue
        rm.promise.success((1 to 5).map(event(_, Instant.now(), None)))
      }

      (1 to 15).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 15
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 10L)
    }

    "replay from a snapshot before both journal boundaries" in new Setup(this) {
      journal ! replayMessagesReq(4, Long.MaxValue, Long.MaxValue)
      hotJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 3
        rhsn.promise.success(15)
      }
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 4
        rm.toSequenceNr shouldBe 15
        rm.promise.success((11 to 15).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5, 1 -> 10))))))
      }

      lukewarmJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 5
        rhsn.promise.success(10)
      }
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 6
        rm.toSequenceNr shouldBe 10
        rm.max shouldBe Long.MaxValue
        rm.promise.success((6 to 10).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }

      coldJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 3
        rhsn.promise.success(5)
      }
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 4
        rm.toSequenceNr shouldBe 5
        rm.max shouldBe Long.MaxValue
        rm.promise.success((4 to 5).map(event(_, Instant.now(), None)))
      }

      (4 to 15).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 15
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 10L)
    }

    "use metadata to replay only from the relevant cooler journal" in new Setup(this) {
      journal ! replayMessagesReq(7, 9, Long.MaxValue)
      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 7
        rm.toSequenceNr shouldBe 9
        rm.promise.success(Nil)
      }
      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 15
        rm.max shouldBe 1
        rm.promise.success(Seq(event(15, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5, 1 -> 10))))))
      }

      lukewarmJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { rhsn =>
        rhsn.fromSequenceNr shouldBe 6
        rhsn.promise.success(10)
      }
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 7
        rm.toSequenceNr shouldBe 9
        rm.promise.success((7 to 9).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }

      (7 to 9).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 15
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 10L)
    }

    "support bounded replays across both journal boundaries" in new Setup(this) {
      journal ! replayMessagesReq(3, 13, 11)
      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 3
        rm.toSequenceNr shouldBe 13
        rm.max shouldBe 11
        rm.promise.success((11 to 13).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5, 1 -> 10))))))
      }
      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe -1
        rm.toSequenceNr shouldBe 15
        rm.max shouldBe 1
        rm.promise.success(Seq(event(15, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5, 1 -> 10))))))
      }

      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 6
        rm.toSequenceNr shouldBe 10
        rm.max shouldBe 11
        rm.promise.success((6 to 10).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
      }

      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(5)
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 3
        rm.toSequenceNr shouldBe 5
        rm.max shouldBe 11
        rm.promise.success((3 to 5).map(event(_, Instant.now(), None)))
      }

      (3 to 13).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 15
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 10L)
    }

    "replay hot events when the start of history is deleted/expired and the cooler journals have no events" in new Setup(
      this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(5)
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 5
        // events 1 and 2 were deleted/expired
        rm.promise.success((3 to 5).map(event(_, Instant.now(), Some(MultiJournalMetadata.Empty))))
      }

      // no events, so the journal doesn't get a ReplayMessages
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)

      (3 to 5).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 5
      getMetadata(pid).futureValue shouldBe Map.empty
    }

    "bound the cold replay by the earliest buffered event when the lukewarm journal has no events" in new Setup(this) {
      journal ! replayMessagesReq(1, Long.MaxValue, Long.MaxValue)
      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 10
        // pid was idle while the lukewarm journal was hot
        rm.promise.success((6 to 10).map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5, 1 -> 5))))))
      }

      // cold journal reports events beyond the buffered hot events (e.g. events 6 and 7 were copied there):
      // the replay must still stop before the earliest buffered event
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(7)
      coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
        rm.fromSequenceNr shouldBe 1
        rm.toSequenceNr shouldBe 5
        rm.max shouldBe Long.MaxValue
        rm.promise.success((1 to 5).map(event(_, Instant.now(), None)))
      }

      (1 to 10).foreach { seqNr =>
        actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
          replayed.persistent.sequenceNr shouldBe seqNr
          replayed.persistent.metadata shouldBe empty
        }
      }
      actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 10
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 5L)
    }

    // replaying 3 to 13: events 3-5 are in cold, 6-10 in lukewarm, 11-13 (of 15) in hot
    Seq(4L, 10L, 11L, Long.MaxValue).foreach { max =>
      s"limit bounded replays across both journal boundaries to max [$max]" in new Setup(this) {
        val expected = (3L to 13L).take(max.min(Int.MaxValue).toInt)

        journal ! replayMessagesReq(3, 13, max)
        hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
        hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
          rm.fromSequenceNr shouldBe 3
          rm.toSequenceNr shouldBe 13
          rm.max shouldBe max
          rm.promise.success(
            (11 to 13)
              .take(max.min(Int.MaxValue).toInt)
              .map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5, 1 -> 10))))))
        }
        hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
        hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
          rm.fromSequenceNr shouldBe -1
          rm.toSequenceNr shouldBe 15
          rm.max shouldBe 1
          rm.promise.success(Seq(event(15, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5, 1 -> 10))))))
        }

        lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
        lukewarmJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
          rm.fromSequenceNr shouldBe 6
          rm.toSequenceNr shouldBe 10
          rm.max shouldBe max
          rm.promise.success(
            (6 to 10)
              .take(max.min(Int.MaxValue).toInt)
              .map(event(_, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5))))))
        }

        coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(5)
        coldJournalProbe.asserting[TestProbeJournal.ReplayMessages] { rm =>
          rm.fromSequenceNr shouldBe 3
          rm.toSequenceNr shouldBe 5
          rm.max shouldBe max
          rm.promise.success((3 to 5).take(max.min(Int.MaxValue).toInt).map(event(_, Instant.now(), None)))
        }

        expected.foreach { seqNr =>
          actorProbe.asserting[JournalProtocol.ReplayedMessage] { replayed =>
            replayed.persistent.sequenceNr shouldBe seqNr
            replayed.persistent.metadata shouldBe empty
          }
        }
        actorProbe.expectMsgType[JournalProtocol.RecoverySuccess].highestSequenceNr shouldBe 15
        getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 10L)
      }
    }
  }
}
