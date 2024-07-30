/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.journal

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.CapabilityFlag
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.JournalProtocol.DeleteMessagesTo
import akka.persistence.JournalProtocol.RecoverySuccess
import akka.persistence.JournalProtocol.ReplayMessages
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.journal.JournalSpec
import akka.testkit.TestProbe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.Inspectors
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually

object DynamoDBJournalSpec {
  val config = DynamoDBJournalSpec.testConfig()

  def configWithMeta =
    ConfigFactory
      .parseString("""akka.persistence.dynamodb.with-meta = true""")
      .withFallback(DynamoDBJournalSpec.testConfig())

  def configWithTTLForDeletes: Config =
    ConfigFactory
      .parseString("""
        akka.persistence.dynamodb.time-to-live {
          # check expiry and set zero TTL for testing as if deleted immediately
          check-expiry = on
          use-time-to-live-for-deletes = 0 seconds
        }
      """)
      .withFallback(DynamoDBJournalSpec.testConfig())

  def configWithEventTTL: Config =
    ConfigFactory
      .parseString("""
        akka.persistence.dynamodb.time-to-live {
          check-expiry = on
          event-time-to-live = 1 hour
        }
      """)
      .withFallback(DynamoDBJournalSpec.testConfig())

  def testConfig(): Config = {
    ConfigFactory
      .parseString(s"""
      # allow java serialization when testing
      akka.actor.allow-java-serialization = on
      akka.actor.warn-about-java-serializer-usage = off
      """)
      .withFallback(TestConfig.config)
  }
}

abstract class DynamoDBJournalBaseSpec(config: Config)
    extends JournalSpec(config)
    with TestDbLifecycle
    with Eventually {

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.off()

  override def typedSystem: ActorSystem[_] = system.toTyped

  // always eventually consistent reads with dynamodb-local, wait for writes to be seen
  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
    val probe = TestProbe()
    eventually(timeout(3.seconds), interval(100.millis)) {
      journal ! ReplayMessages(fromSnr, toSnr, max = 0, pid, probe.ref)
      probe.expectMsg(RecoverySuccess(toSnr))
    }
  }
}

class DynamoDBJournalSpec extends DynamoDBJournalBaseSpec(DynamoDBJournalSpec.config)

class DynamoDBJournalWithMetaSpec extends DynamoDBJournalBaseSpec(DynamoDBJournalSpec.configWithMeta) {
  protected override def supportsMetadata: CapabilityFlag = CapabilityFlag.on()
}

class DynamoDBJournalWithTTLForDeletesSpec
    extends DynamoDBJournalBaseSpec(DynamoDBJournalSpec.configWithTTLForDeletes)
    with Inspectors
    with OptionValues {

  // run the deletion tests again, with expiry attribute checks

  "A journal with TTL settings" should {

    "not replay expired events that were deleted using time-to-live" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val receiverProbe = TestProbe()
      val receiverProbe2 = TestProbe()
      val cmd = DeleteMessagesTo(pid, 3, receiverProbe2.ref)
      val sub = TestProbe()

      subscribe[DeleteMessagesTo](sub.ref)
      journal ! cmd
      sub.expectMsg(cmd)
      receiverProbe2.expectMsg(DeleteMessagesSuccess(cmd.toSequenceNr))

      val now = System.currentTimeMillis / 1000
      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe 5
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < 3) {
          eventItem.get(Expiry).value.n.toLong should (be <= now and be > now - 10) // within 10s
          eventItem.get(ExpiryMarker) shouldBe None
        } else if (seqNr == 3) { // expiry marker at 3
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong should (be <= now and be > now - 10) // within 10s
        } else {
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      List(4, 5).foreach { i =>
        receiverProbe.expectMsg(replayedMessage(i))
      }

      receiverProbe2.expectNoMessage(200.millis)
    }

    "not reset highestSequenceNr after events were deleted using time-to-live" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val receiverProbe = TestProbe()

      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      (1 to 5).foreach { i =>
        receiverProbe.expectMsg(replayedMessage(i))
      }
      receiverProbe.expectMsg(RecoverySuccess(highestSequenceNr = 5L))

      journal ! DeleteMessagesTo(pid, 3L, receiverProbe.ref)
      receiverProbe.expectMsg(DeleteMessagesSuccess(3L))

      val now = System.currentTimeMillis / 1000
      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe 5
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < 3) {
          eventItem.get(Expiry).value.n.toLong should (be <= now and be > now - 10) // within 10s
          eventItem.get(ExpiryMarker) shouldBe None
        } else if (seqNr == 3) { // expiry marker at 3
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong should (be <= now and be > now - 10) // within 10s
        } else {
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      (4 to 5).foreach { i =>
        receiverProbe.expectMsg(replayedMessage(i))
      }
      receiverProbe.expectMsg(RecoverySuccess(highestSequenceNr = 5L))
    }

    "not reset highestSequenceNr after journal cleanup with events deleted using time-to-live" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val receiverProbe = TestProbe()

      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      (1 to 5).foreach { i =>
        receiverProbe.expectMsg(replayedMessage(i))
      }
      receiverProbe.expectMsg(RecoverySuccess(highestSequenceNr = 5L))

      journal ! DeleteMessagesTo(pid, Long.MaxValue, receiverProbe.ref)
      receiverProbe.expectMsg(DeleteMessagesSuccess(Long.MaxValue))

      val now = System.currentTimeMillis / 1000
      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe 5
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < 5) {
          eventItem.get(Expiry).value.n.toLong should (be <= now and be > now - 10) // within 10s
          eventItem.get(ExpiryMarker) shouldBe None
        } else { // expiry marker at last event
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong should (be <= now and be > now - 10) // within 10s
        }
      }

      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, pid, receiverProbe.ref)
      receiverProbe.expectMsg(RecoverySuccess(highestSequenceNr = 5L))
    }

  }
}

class DynamoDBJournalWithEventTTLSpec
    extends DynamoDBJournalBaseSpec(DynamoDBJournalSpec.configWithEventTTL)
    with Inspectors
    with OptionValues {

  // check persisted events all have expiry set

  override protected def beforeEach(): Unit = {
    import akka.persistence.dynamodb.internal.JournalAttributes._
    super.beforeEach() // test events written
    val expected = System.currentTimeMillis / 1000 + 1.hour.toSeconds
    val eventItems = getEventItemsFor(pid)
    eventItems.size shouldBe 5
    forAll(eventItems) { eventItem =>
      eventItem.get(Expiry).value.n.toLong should (be <= expected and be > expected - 10) // within 10s
      eventItem.get(ExpiryMarker) shouldBe None
    }
  }
}
