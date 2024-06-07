/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.snapshot

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.CapabilityFlag
import akka.persistence.DeleteSnapshotSuccess
import akka.persistence.SnapshotMetadata
import akka.persistence.SnapshotProtocol.DeleteSnapshot
import akka.persistence.SnapshotProtocol.LoadSnapshot
import akka.persistence.SnapshotProtocol.LoadSnapshotResult
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.testkit.TestProbe
import org.scalatest.Outcome
import org.scalatest.Pending

class DynamoDBSnapshotStoreSpec extends SnapshotStoreSpec(TestConfig.config) with TestDbLifecycle {
  def typedSystem: ActorSystem[_] = system.toTyped

  private val ignoreTests = Set(
    // All these expects multiple snapshots for same pid, either as the core test
    // or as a verification that there are still snapshots in db after some specific delete
    "A snapshot store must load the most recent snapshot matching an upper sequence number bound",
    "A snapshot store must load the most recent snapshot matching an upper sequence number bound",
    "A snapshot store must load the most recent snapshot matching upper sequence number and timestamp bounds",
    "A snapshot store must delete a single snapshot identified by sequenceNr in snapshot metadata",
    "A snapshot store must delete all snapshots matching upper sequence number and timestamp bounds",
    "A snapshot store must not delete snapshots with non-matching upper timestamp bounds")

  override protected def withFixture(test: NoArgTest): Outcome =
    if (ignoreTests(test.name)) {
      Pending // No Ignored/Skipped available so Pending will have to do
    } else {
      super.withFixture(test)
    }

  protected override def supportsMetadata: CapabilityFlag = true

  // Note: these depend on populating the database with snapshots in SnapshotStoreSpec.beforeEach
  // mostly covers the important bits of the skipped tests but for an update-in-place snapshot store

  "An update-in-place snapshot store" should {

    "not find any other snapshots than the latest with upper sequence number bound" in {
      // SnapshotStoreSpec saves snapshots with sequence nr 10-15
      val senderProbe = TestProbe()
      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria(maxSequenceNr = 13), Long.MaxValue),
        senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, toSequenceNr = 13), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, 13))
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, toSequenceNr = 15), senderProbe.ref)

      // no access to SnapshotStoreSpec.metadata with timestamps so can't compare directly (because timestamp)
      val result = senderProbe.expectMsgType[LoadSnapshotResult]
      result.snapshot shouldBe defined
      result.snapshot.get.snapshot should ===("s-5")
    }

    "delete the single snapshot for a pid identified by sequenceNr in snapshot metadata" in {
      val senderProbe = TestProbe()

      // first confirm the current sequence number for the snapshot
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria(), Long.MaxValue), senderProbe.ref)
      val result = senderProbe.expectMsgType[LoadSnapshotResult]
      result.snapshot shouldBe defined
      val sequenceNr = result.snapshot.get.metadata.sequenceNr
      sequenceNr shouldBe 15

      val md = SnapshotMetadata(pid, sequenceNr, timestamp = 0)
      val cmd = DeleteSnapshot(md)
      val sub = TestProbe()

      subscribe[DeleteSnapshot](sub.ref)
      snapshotStore.tell(cmd, senderProbe.ref)
      sub.expectMsg(cmd)
      senderProbe.expectMsg(DeleteSnapshotSuccess(md))

      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria(), Long.MaxValue), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
    }
  }
}
