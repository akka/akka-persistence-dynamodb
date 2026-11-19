/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.DeleteMessagesFailure
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.JournalProtocol
import akka.persistence.Persistence
import akka.testkit.TestProbe
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

class DeleteSpec
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

  def deleteMessages(setup: Setup, toSequenceNr: Long): Unit =
    deleteMessages(setup, toSequenceNr, setup.actorProbe)

  def deleteMessages(setup: Setup, toSequenceNr: Long, probe: TestProbe): Unit =
    journal ! JournalProtocol.DeleteMessagesTo(setup.pid, toSequenceNr, probe.ref)

  def recoverMetadataFromHot(setup: Setup): Unit = {
    import setup._
    hotJournalProbe.asserting[TestProbeJournal.ReadHighestSeqNr] { read =>
      read.pid shouldBe pid
      read.fromSequenceNr shouldBe 0
      read.promise.success(15)
    }
    hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { replay =>
      replay.pid shouldBe pid
      replay.fromSequenceNr shouldBe -1
      replay.toSequenceNr shouldBe 15
      replay.max shouldBe 1
      replay.promise.success(Seq(event(15, Instant.now(), Some(MultiJournalMetadata(Map(0 -> 5L, 1 -> 10L))))))
    }
  }

  override def afterEach(): Unit = {
    super.afterEach()
    coldJournalProbe.expectNoMessage(10.millis)
    lukewarmJournalProbe.expectNoMessage(10.millis)
    hotJournalProbe.expectNoMessage(10.millis)
  }

  "MultiJournal deletion" should {
    "recover empty metadata and complete a no-op deletion" in new Setup(this) {
      deleteMessages(this, 0)

      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)
      coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)

      actorProbe.expectMsg(DeleteMessagesSuccess(0))
      getMetadata(pid).futureValue shouldBe Map.empty

      // Cached empty metadata makes the second deletion an immediate no-op.
      deleteMessages(this, 0)
      actorProbe.expectMsg(DeleteMessagesSuccess(0))
    }

    "delete from the hot journal when metadata is empty" in new Setup(this) {
      deleteMessages(this, 10)

      hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(10)
      hotJournalProbe.asserting[TestProbeJournal.ReplayMessages] { replay =>
        replay.fromSequenceNr shouldBe -1
        replay.toSequenceNr shouldBe 10
        replay.max shouldBe 1
        replay.promise.success(Seq(event(10, Instant.now(), Some(MultiJournalMetadata.Empty))))
      }
      hotJournalProbe.asserting[TestProbeJournal.DeleteMessages] { delete =>
        delete.persistenceId shouldBe pid
        delete.toSequenceNr shouldBe 10
        delete.promise.success(())
      }

      actorProbe.expectMsg(DeleteMessagesSuccess(10))
      getMetadata(pid).futureValue shouldBe Map.empty
    }

    "route deletion through the journal containing the upper bound" in new Setup(this) {
      deleteMessages(this, 8)
      recoverMetadataFromHot(this)

      coldJournalProbe.asserting[TestProbeJournal.DeleteMessages] { delete =>
        delete.persistenceId shouldBe pid
        delete.toSequenceNr shouldBe 8
        delete.promise.success(())
      }
      lukewarmJournalProbe.asserting[TestProbeJournal.DeleteMessages] { delete =>
        delete.persistenceId shouldBe pid
        delete.toSequenceNr shouldBe 8
        delete.promise.success(())
      }

      actorProbe.expectMsg(DeleteMessagesSuccess(8))
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 10L)
    }

    "report a partial deletion failure and stop deleting hotter journals" in new Setup(this) {
      deleteMessages(this, 12)
      recoverMetadataFromHot(this)

      coldJournalProbe.expectMessageType[TestProbeJournal.DeleteMessages].promise.success(())
      val failure = new RuntimeException("lukewarm journal failed deletion")
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.DeleteMessages].promise.failure(failure)

      actorProbe.expectMsgType[DeleteMessagesFailure].cause shouldBe failure
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 10L)
    }

    "delay a second deletion until the active deletion completes" in new Setup(this) {
      val secondProbe = TestProbe()
      deleteMessages(this, 8)
      deleteMessages(this, 4, secondProbe)
      recoverMetadataFromHot(this)

      coldJournalProbe.expectMessageType[TestProbeJournal.DeleteMessages].promise.success(())
      lukewarmJournalProbe.expectMessageType[TestProbeJournal.DeleteMessages].promise.success(())
      actorProbe.expectMsg(DeleteMessagesSuccess(8))

      coldJournalProbe.asserting[TestProbeJournal.DeleteMessages] { delete =>
        delete.persistenceId shouldBe pid
        delete.toSequenceNr shouldBe 4
        delete.promise.success(())
      }
      secondProbe.expectMsg(DeleteMessagesSuccess(4))
      getMetadata(pid).futureValue shouldBe Map(0 -> 5L, 1 -> 10L)
    }
  }
}
