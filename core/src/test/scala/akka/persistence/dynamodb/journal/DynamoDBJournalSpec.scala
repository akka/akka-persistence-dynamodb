/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.journal

import java.time.Instant
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Random

import akka.actor.ActorRef
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.{ TestProbe => TypedTestProbe }
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.AtomicWrite
import akka.persistence.CapabilityFlag
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.FallbackStoreProvider
import akka.persistence.InMemFallbackStore
import akka.persistence.JournalProtocol.DeleteMessagesTo
import akka.persistence.JournalProtocol.RecoverySuccess
import akka.persistence.JournalProtocol.ReplayMessages
import akka.persistence.JournalProtocol.WriteMessages
import akka.persistence.JournalProtocol.WriteMessagesSuccessful
import akka.persistence.PersistentRepr
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.journal.JournalSpec
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.Inspectors
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import akka.persistence.JournalProtocol.ReplayedMessage

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
          event-sourced-entities {
            "*" {
              # check expiry by default and set zero TTL for testing as if deleted immediately
              use-time-to-live-for-deletes = 0 seconds
            }
          }
        }
      """)
      .withFallback(DynamoDBJournalSpec.testConfig())

  def configWithEventTTL: Config =
    ConfigFactory
      .parseString("""
        akka.persistence.dynamodb.time-to-live {
          event-sourced-entities {
            "*" {
              event-time-to-live = 1 hour
            }
          }
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

  def configWithSmallFallback: Config =
    ConfigFactory
      .parseString("""
      fallback-plugin {
        class = "akka.persistence.InMemFallbackStore"
      }

      akka.persistence.dynamodb.journal.fallback-store {
        plugin = "fallback-plugin"
        threshold = 4 KiB
      }
      """)
      .withFallback(TestConfig.config)
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

class DynamoDBJournalWithFallbackSpec
    extends ScalaTestWithActorTestKit(DynamoDBJournalSpec.configWithSmallFallback)
    with AnyWordSpecLike
    with TestData
    with TestDbLifecycle
    with LogCapturing {
  import InMemFallbackStore.Invocation
  import akka.persistence.dynamodb.internal.JournalAttributes._

  def typedSystem: ActorSystem[_] = testKit.system

  class Setup {
    val entityType = nextEntityType()
    val persistenceId = nextPersistenceId(entityType)
    val journal = persistenceExt.journalFor("akka.persistence.dynamodb.journal")
    val slice = persistenceExt.sliceForPersistenceId(persistenceId.id)
    val writerUuid = UUID.randomUUID()
  }

  "A DynamoDB journal with fallback enabled" should {
    "not interact with the fallback store for small events" in withFallbackStoreProbe { (fallbackStore, invocations) =>
      new Setup {
        val probe = TypedTestProbe[Any]()
        val classicProbeRef = probe.ref.toClassic

        journal.tell(
          WriteMessages(Seq(AtomicWrite(Seq(PersistentRepr("event", 1, persistenceId.id)))), classicProbeRef, 1),
          classicProbeRef)

        probe.expectMessageType[WriteMessagesSuccessful.type]
        invocations.expectNoMessage(10.millis)
      }
    }

    "save a large event to the fallback store and save the breadcrumb" in withFallbackStoreProbe {
      (fallbackStore, invocations) =>
        new Setup {
          val probe = TypedTestProbe[Any]()
          val classicProbeRef = probe.ref.toClassic

          val bigArr = Random.nextBytes(3072)

          journal.tell(
            WriteMessages(Seq(AtomicWrite(Seq(PersistentRepr(bigArr, 1, persistenceId.id)))), classicProbeRef, 1),
            classicProbeRef)

          val invocation = invocations.expectNext()
          invocation.method shouldBe "saveEvent"
          invocation.args(0) shouldBe persistenceId.id
          invocation.args(1) shouldBe 1 // seqNr
          invocation.args(2) shouldBe 4 // serId: ByteArraySerializer
          invocation.args(3) shouldBe "" // serManifest: (ByteArraySerializer.includeManifest = false)
          (invocation.args(4).asInstanceOf[Array[Byte]] should contain).theSameElementsInOrderAs(bigArr)

          probe.expectMessageType[WriteMessagesSuccessful.type]

          val maybeEvents = getEventItemsFor(persistenceId.id)
          maybeEvents shouldNot be(empty)
          maybeEvents.flatMap(_.get(EventPayload)) should be(empty)
          maybeEvents.foreach { evt =>
            new String(evt.get(BreadcrumbPayload).get.b.asByteArray) shouldBe "breadcrumb"
          }
        }
    }

    "follow the breadcrumb to an event" in withFallbackStoreProbe { (fallbackStore, invocations) =>
      new Setup {
        val probe = TypedTestProbe[Any]()
        val classicProbeRef = probe.ref.toClassic

        val arr = Random.nextBytes(8)

        Await.result(
          fallbackStore
            .saveEvent(persistenceId.id, 1, 4, "", arr)
            .flatMap { breadcrumb =>
              breadcrumb shouldBe "breadcrumb"

              val ddbClient = ClientProvider(system).clientFor("akka.persistence.dynamodb.client")
              val serialization = SerializationExtension(system)

              val serializer = serialization.findSerializerFor(breadcrumb)
              val bytes = serializer.toBinary(breadcrumb)
              val manifest = Serializers.manifestFor(serializer, breadcrumb)

              val attributes = new java.util.HashMap[String, AttributeValue]
              attributes.put(Pid, AttributeValue.fromS(persistenceId.id))
              attributes.put(SeqNr, AttributeValue.fromN("1"))
              attributes.put(EntityTypeSlice, AttributeValue.fromS(s"$entityType-$slice"))
              attributes.put(Timestamp, AttributeValue.fromN(InstantFactory.toEpochMicros(Instant.now()).toString))
              attributes.put(Writer, AttributeValue.fromS(writerUuid.toString))
              attributes.put(BreadcrumbSerId, AttributeValue.fromN(serializer.identifier.toString))
              attributes.put(BreadcrumbSerManifest, AttributeValue.fromS(manifest))
              attributes.put(BreadcrumbPayload, AttributeValue.fromB(SdkBytes.fromByteArray(bytes)))

              ddbClient
                .putItem(PutItemRequest.builder.tableName(settings.journalTable).item(attributes).build)
                .asScala
                .map { _ =>
                  invocations.expectNext(10.millis).method
                }(system.executionContext)
            }(system.executionContext),
          1.second) shouldBe "saveEvent"

        journal.tell(ReplayMessages(1, 10, 10, persistenceId.id, classicProbeRef), classicProbeRef)

        //invocations.requestNext()
        invocations.requestNext() shouldBe Invocation("toBreadcrumb", Seq("breadcrumb"))
        val loadInvocation = invocations.requestNext(10.millis)
        loadInvocation.method shouldBe "loadEvent"
        loadInvocation.args(0) shouldBe "breadcrumb"
        loadInvocation.args(1) shouldBe persistenceId.id
        loadInvocation.args(2) shouldBe 1
        loadInvocation.args(3) shouldBe true

        val result = probe.expectMessageType[ReplayedMessage]
        (result.persistent.payload.asInstanceOf[Array[Byte]] should contain).theSameElementsInOrderAs(arr)
        result.persistent.persistenceId shouldBe persistenceId.id
        result.persistent.sequenceNr shouldBe 1
      }
    }
  }

  private def withFallbackStoreProbe(
      test: (InMemFallbackStore, TestSubscriber.Probe[InMemFallbackStore.Invocation]) => Unit): Unit = {

    val fallbackStore =
      FallbackStoreProvider(testKit.system).fallbackStoreFor("fallback-plugin").asInstanceOf[InMemFallbackStore]

    val invocationsProbe = fallbackStore.invocationStream.runWith(TestSink()(testKit.system))

    invocationsProbe.ensureSubscription()
    invocationsProbe.request(1)
    test(fallbackStore, invocationsProbe)
    invocationsProbe.cancel()
  }
}
