/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.snapshot

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Random

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.{ TestProbe => TypedTestProbe }
import akka.persistence.CapabilityFlag
import akka.persistence.DeleteSnapshotSuccess
import akka.persistence.FallbackStoreProvider
import akka.persistence.InMemFallbackStore
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotMetadata
import akka.persistence.SnapshotProtocol.DeleteSnapshot
import akka.persistence.SnapshotProtocol.LoadSnapshot
import akka.persistence.SnapshotProtocol.LoadSnapshotResult
import akka.persistence.SnapshotProtocol.SaveSnapshot
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.dynamodb.internal.SnapshotAttributes
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.TestSubscriber
import akka.testkit.TestProbe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.OptionValues
import org.scalatest.Outcome
import org.scalatest.Pending
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

object DynamoDBSnapshotStoreSpec {
  val config: Config = TestConfig.config

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
      .withFallback(config)

  def configWithSnapshotTTL: Config =
    ConfigFactory
      .parseString("""
        akka.persistence.dynamodb.time-to-live {
          event-sourced-entities {
            "*" {
              snapshot-time-to-live = 1 hour
            }
          }
        }
      """)
      .withFallback(config)

  def configWithSmallFallback: Config =
    ConfigFactory
      .parseString("""
      fallback-plugin {
        class = "akka.persistence.InMemFallbackStore"
      }

      akka.persistence.dynamodb.snapshot.fallback-store {
        plugin = "fallback-plugin"
        threshold = 4 KiB
      }
      """)
      .withFallback(config)
}

abstract class DynamoDBSnapshotStoreBaseSpec(config: Config)
    extends SnapshotStoreSpec(config)
    with TestDbLifecycle
    with OptionValues {

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

  protected def usingTTLForDeletes: Boolean = false

  protected def usingSnapshotTTL: Boolean = false

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

      if (usingSnapshotTTL) {
        val expected = System.currentTimeMillis / 1000 + 1.hour.toSeconds
        val snapshotItem = getSnapshotItemFor(pid).value
        snapshotItem.get(SnapshotAttributes.Expiry).value.n.toLong should (be <= expected and be > expected - 10)
      }

      val md = SnapshotMetadata(pid, sequenceNr, timestamp = 0)
      val cmd = DeleteSnapshot(md)
      val sub = TestProbe()

      subscribe[DeleteSnapshot](sub.ref)
      snapshotStore.tell(cmd, senderProbe.ref)
      sub.expectMsg(cmd)
      senderProbe.expectMsg(DeleteSnapshotSuccess(md))

      if (usingTTLForDeletes) {
        val now = System.currentTimeMillis / 1000
        val snapshotItem = getSnapshotItemFor(pid).value
        snapshotItem.get(SnapshotAttributes.Expiry).value.n.toLong should (be <= now and be > now - 10) // within 10s
      }

      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria(), Long.MaxValue), senderProbe.ref)
      senderProbe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
    }
  }
}

class DynamoDBSnapshotStoreSpec extends DynamoDBSnapshotStoreBaseSpec(DynamoDBSnapshotStoreSpec.config)

class DynamoDBSnapshotStoreWithTTLForDeletesSpec
    extends DynamoDBSnapshotStoreBaseSpec(DynamoDBSnapshotStoreSpec.configWithTTLForDeletes) {
  override protected def usingTTLForDeletes: Boolean = true
}

class DynamoDBSnapshotStoreWithSnapshotTTLSpec
    extends DynamoDBSnapshotStoreBaseSpec(DynamoDBSnapshotStoreSpec.configWithSnapshotTTL) {
  override protected def usingSnapshotTTL: Boolean = true
}

class DynamoDBSnapshotFallbackSpec
    extends ScalaTestWithActorTestKit(DynamoDBSnapshotStoreSpec.configWithSmallFallback)
    with AnyWordSpecLike
    with TestData
    with TestDbLifecycle
    with LogCapturing {
  import InMemFallbackStore.Invocation
  import SnapshotAttributes._

  def typedSystem: ActorSystem[_] = testKit.system

  class Setup {
    val entityType = nextEntityType()
    val persistenceId = nextPersistenceId(entityType)
    val snapshotStore = persistenceExt.snapshotStoreFor("akka.persistence.dynamodb.snapshot")
  }

  "A DynamoDB snapshot store with fallback enabled" should {
    "not interact with the fallback store for small snapshots" in withFallbackStoreProbe {
      (fallbackStore, invocations) =>
        new Setup {
          val probe = TypedTestProbe[Any]()
          val classicProbeRef = probe.ref.toClassic

          snapshotStore.tell(SaveSnapshot(SnapshotMetadata(persistenceId.id, 1, 0, None), "small"), classicProbeRef)

          probe.expectMessageType[SaveSnapshotSuccess]
          invocations.expectNoMessage(10.millis)
        }
    }

    "save a large snapshot to the fallback store and save the breadcrumb" in withFallbackStoreProbe {
      (fallbackStore, invocations) =>
        new Setup {
          val probe = TypedTestProbe[Any]()
          val classicProbeRef = probe.ref.toClassic

          val bigArr = Random.nextBytes(3072) // less than 4k, but the base64 encoding adds overhead

          snapshotStore.tell(SaveSnapshot(SnapshotMetadata(persistenceId.id, 2, 0, None), bigArr), classicProbeRef)

          val invocation = invocations.expectNext()
          val now = Instant.now()
          invocation.method shouldBe "saveSnapshot"
          invocation.args(0) shouldBe persistenceId.id
          invocation.args(1) shouldBe 2 // seqNr
          assert(now.compareTo(invocation.args(2).asInstanceOf[Instant]) >= 0, "writeTimestamp should be before now")
          assert(now.compareTo(invocation.args(3).asInstanceOf[Instant]) >= 0, "eventTimestamp should be before now")
          invocation.args(4) shouldBe 4 // serId: ByteArraySerializer
          invocation.args(5) shouldBe "" // serManifest (ByteArraySerializer.includeManifest = false)
          (invocation.args(6).asInstanceOf[Array[Byte]] should contain).theSameElementsInOrderAs(bigArr)
          invocation.args(7).asInstanceOf[Set[String]] shouldBe empty
          invocation.args(8) shouldBe None

          probe.expectMessageType[SaveSnapshotSuccess]

          val maybeSnapshot = getSnapshotItemFor(persistenceId.id)
          maybeSnapshot shouldNot be(empty)
          maybeSnapshot.flatMap(_.get(SnapshotPayload)) should be(empty)
          maybeSnapshot.foreach { snap =>
            new String(snap.get(BreadcrumbPayload).get.b.asByteArray) shouldBe "breadcrumb"
          }
        }
    }

    "follow the breadcrumb to a snapshot" in withFallbackStoreProbe { (fallbackStore, invocations) =>
      new Setup {
        val slice = persistenceExt.sliceForPersistenceId(persistenceId.id)
        val probe = TypedTestProbe[Any]()
        val classicProbeRef = probe.ref.toClassic

        val arr = Random.nextBytes(8)
        val now = Instant.now()

        Await.result(
          fallbackStore
            .saveSnapshot(persistenceId.id, 3, now, now, 4, "", arr, Set.empty, None)
            .flatMap { breadcrumb =>
              breadcrumb shouldBe "breadcrumb"

              val ddbClient = ClientProvider(system).clientFor("akka.persistence.dynamodb.client")
              val serialization = SerializationExtension(system)

              val serializer = serialization.findSerializerFor(breadcrumb)
              val bytes = serializer.toBinary(breadcrumb)
              val manifest = Serializers.manifestFor(serializer, breadcrumb)

              val attributes = new java.util.HashMap[String, AttributeValue]
              attributes.put(Pid, AttributeValue.fromS(persistenceId.id))
              attributes.put(SeqNr, AttributeValue.fromN("3"))
              attributes.put(EntityTypeSlice, AttributeValue.fromS(s"$entityType-$slice"))
              attributes.put(BreadcrumbSerId, AttributeValue.fromN(serializer.identifier.toString))
              attributes.put(BreadcrumbSerManifest, AttributeValue.fromS(manifest))
              attributes.put(BreadcrumbPayload, AttributeValue.fromB(SdkBytes.fromByteArray(bytes)))

              ddbClient
                .putItem(PutItemRequest.builder.tableName(settings.snapshotTable).item(attributes).build)
                .asScala
                .map { _ =>
                  invocations.expectNext(10.millis).method
                }(system.executionContext)
            }(system.executionContext),
          1.second) shouldBe "saveSnapshot"

        snapshotStore.tell(
          LoadSnapshot(persistenceId.id, SnapshotSelectionCriteria.Latest, Long.MaxValue),
          classicProbeRef)

        invocations.requestNext() shouldBe Invocation("toBreadcrumb", Seq("breadcrumb"))
        val loadInvocation = invocations.requestNext(10.millis)
        loadInvocation.method shouldBe "loadSnapshot"
        loadInvocation.args(0) shouldBe "breadcrumb"
        loadInvocation.args(1) shouldBe persistenceId.id
        loadInvocation.args(2) shouldBe 3

        val result = probe.expectMessageType[LoadSnapshotResult]
        result.toSequenceNr shouldBe Long.MaxValue
        result.snapshot shouldNot be(empty)
        (result.snapshot.get.snapshot.asInstanceOf[Array[Byte]] should contain).theSameElementsInOrderAs(arr)
        result.snapshot.get.metadata.persistenceId shouldBe persistenceId.id
        result.snapshot.get.metadata.sequenceNr shouldBe 3
        result.snapshot.get.metadata.timestamp shouldBe now.toEpochMilli
        result.snapshot.get.metadata.metadata shouldBe empty
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
