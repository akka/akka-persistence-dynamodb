/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import java.time.Instant

import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.AtomicWrite
import akka.persistence.CompositeMetadata
import akka.persistence.JournalProtocol
import akka.persistence.Persistence
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

class WriteSpec
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

  def writeMessages(setup: Setup, writes: Seq[AtomicWrite], instanceId: Int): Unit =
    journal ! JournalProtocol.WriteMessages(writes, setup.actorProbe.ref, instanceId)

  def recoverEmptyMetadata(): Unit = {
    hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)
    lukewarmJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)
    coldJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(0)
  }

  def recoverMetadataFromHot(setup: Setup): Unit = {
    import setup._
    hotJournalProbe.expectMessageType[TestProbeJournal.ReadHighestSeqNr].promise.success(15)
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

  "MultiJournal writes" should {
    "return one result per atomic write containing multiple events" in new Setup(this) {
      val instanceId = 101
      val firstWrite = AtomicWrite(Seq(event(1, Instant.now(), None), event(2, Instant.now(), None)))
      val secondWrite = AtomicWrite(Seq(event(3, Instant.now(), None)))
      writeMessages(this, Seq(firstWrite, secondWrite), instanceId)
      recoverEmptyMetadata()

      val rejection = new RuntimeException("second atomic write rejected")
      hotJournalProbe.asserting[TestProbeJournal.WriteMessages] { write =>
        write.messages.size shouldBe 2
        write.messages.map(_.payload.size) shouldBe Seq(2, 1)
        write.messages.iterator.flatMap(_.payload).foreach { repr =>
          repr.metadata shouldBe Some(MultiJournalMetadata.Empty)
        }
        write.promise.success(Seq(Success(()), Failure(rejection)))
      }

      actorProbe.expectMsg(JournalProtocol.WriteMessagesSuccessful)
      actorProbe.expectMsgType[JournalProtocol.WriteMessageSuccess].persistent.sequenceNr shouldBe 1
      actorProbe.expectMsgType[JournalProtocol.WriteMessageSuccess].persistent.sequenceNr shouldBe 2
      actorProbe.asserting[JournalProtocol.WriteMessageRejected] { rejected =>
        rejected.message.sequenceNr shouldBe 3
        rejected.cause shouldBe rejection
        rejected.actorInstanceId shouldBe instanceId
      }
    }

    "reject a batch containing multiple persistence IDs" in new Setup(this) {
      val instanceId = 202
      val otherPid = nextPid()
      val firstWrite = AtomicWrite(Seq(event(1, Instant.now(), None)))
      val secondWrite = AtomicWrite(Seq(event(1, Instant.now(), None).update(persistenceId = otherPid)))
      writeMessages(this, Seq(firstWrite, secondWrite), instanceId)

      actorProbe.asserting[JournalProtocol.WriteMessagesFailed] { failed =>
        failed.cause.getMessage shouldBe "Multiple persistence IDs per WriteMessages is not supported"
        failed.writeCount shouldBe 2
      }
      actorProbe.receiveN(2).foreach { message =>
        val failed = message.asInstanceOf[JournalProtocol.WriteMessageFailure]
        failed.cause.getMessage shouldBe "Multiple persistence IDs per WriteMessages is not supported"
        failed.actorInstanceId shouldBe instanceId
      }
    }

    "replace internal metadata while preserving user metadata" in new Setup(this) {
      val instanceId = 303
      val oldMetadata = MultiJournalMetadata(Map(0 -> 2L))
      val expectedMetadata = MultiJournalMetadata(Map(0 -> 5L, 1 -> 10L))
      val userMetadata = "user-metadata"
      val otherUserMetadata = 42L
      val writes = Seq(
        AtomicWrite(Seq(event(16, Instant.now(), Some(oldMetadata)))),
        AtomicWrite(Seq(event(17, Instant.now(), Some(userMetadata)))),
        AtomicWrite(
          Seq(
            event(18, Instant.now(), Some(new CompositeMetadata(Seq(oldMetadata, userMetadata, otherUserMetadata)))))))

      writeMessages(this, writes, instanceId)
      recoverMetadataFromHot(this)

      hotJournalProbe.asserting[TestProbeJournal.WriteMessages] { write =>
        val persisted = write.messages.map(_.payload.head)
        persisted.head.metadata shouldBe Some(expectedMetadata)

        persisted(1).metadata shouldBe Some(new CompositeMetadata(Seq(expectedMetadata, userMetadata)))
        persisted(2).metadata shouldBe Some(
          new CompositeMetadata(Seq(expectedMetadata, userMetadata, otherUserMetadata)))
        write.promise.success(Nil)
      }

      actorProbe.expectMsg(JournalProtocol.WriteMessagesSuccessful)
      actorProbe.receiveN(3).iterator.zipWithIndex.foreach { case (message, index) =>
        val success = message.asInstanceOf[JournalProtocol.WriteMessageSuccess]
        success.persistent.sequenceNr shouldBe (16 + index)
        success.actorInstanceId shouldBe instanceId
      }
    }
  }
}
