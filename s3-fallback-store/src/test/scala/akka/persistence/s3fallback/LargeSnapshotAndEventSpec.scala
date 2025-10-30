/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.s3fallback

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.TestActors
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.internal.EnvelopeOrigin
import akka.persistence.dynamodb.internal.JournalAttributes
import akka.persistence.dynamodb.internal.SnapshotAttributes
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import io.minio.CopyObjectArgs
import io.minio.CopySource
import io.minio.GetObjectArgs
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.io.Source
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Random

object LargeSnapshotAndEventSpec {
  val config =
    ConfigFactory.parseResources("application-test.conf").withFallback(ConfigFactory.defaultReference).resolve
}

class LargeSnapshotAndEventSpec
    extends ScalaTestWithActorTestKit(LargeSnapshotAndEventSpec.config)
    with AnyWordSpecLike
    with TestData
    with TestDbAndS3Lifecycle
    with LogCapturing {
  //import LargeSnapshotAndEventSpec._
  import TestActors.Persister
  import Persister._

  override def typedSystem: ActorSystem[_] = system

  val query = PersistenceQuery(testKit.system).readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)

  val s3Threshold = system.settings.config.getBytes("akka.persistence.dynamodb.journal.fallback-store.threshold")
  val snapshotBucket = system.settings.config.getString("akka.persistence.s3-fallback-store.snapshots-bucket")
  val eventBucket = system.settings.config.getString("akka.persistence.s3-fallback-store.events-bucket")

  val ddbClient = ClientProvider(system).clientFor("akka.persistence.dynamodb.client")
  val ddbClientSettings = ClientProvider(system).clientSettingsFor("akka.persistence.dynamodb.client")

  class Setup {
    val entityType = nextEntityType()
    val persistenceId = nextPersistenceId(entityType)
    val slice = query.sliceForPersistenceId(persistenceId.id)
    val persister = spawn(Persister(persistenceId))
    val probe = createTestProbe[Done]()
    val sinkProbe = TestSink[EventEnvelope[String]]()(system.classicSystem)
  }

  "DynamoDB S3 fallback" should {
    s"write snapshots larger than the threshold ($s3Threshold) to S3" in new Setup {
      // prime the pump
      for (i <- 1 to 140) {
        val padding = Iterator.continually(Random.nextPrintableChar()).take(30).mkString("").replaceAll("snap", "")

        persister ! PersistWithAck(s"e-$i: $padding", probe.ref)
        probe.expectMessage(Done)
      }

      // Accumulated state now exceeds 4k
      persister ! PersistWithAck(s"e-141: snap to it", probe.ref)
      probe.expectMessage(Done)

      val nextPid = nextPersistenceId(entityType)

      whenReady(
        minioClient
          .getObject(
            GetObjectArgs.builder
              .bucket(snapshotBucket)
              .`object`(s"${persistenceId.entityTypeHint}/${persistenceId.entityId}")
              .build)
          .asScala) { response =>
        val size = Source.fromInputStream(response).size
        assert(size > s3Threshold, "size should exceed S3 threshold")

        whenReady(getSnapshotItemFromDynamo(persistenceId.id)) { item =>
          import SnapshotAttributes._

          item.keySet.intersect(Set(SnapshotSerId, SnapshotSerManifest, SnapshotPayload)) shouldBe empty
          item.keySet.intersect(Set(BreadcrumbSerId, BreadcrumbSerManifest, BreadcrumbPayload)).size shouldBe 3

          // Now test recovery by copying the snapshot with no events
          whenReady(
            minioClient
              .copyObject(
                CopyObjectArgs.builder
                  .bucket(snapshotBucket)
                  .`object`(s"${nextPid.entityTypeHint}/${nextPid.entityId}")
                  .source(CopySource.builder
                    .bucket(snapshotBucket)
                    .`object`(s"${persistenceId.entityTypeHint}/${persistenceId.entityId}")
                    .build)
                  .build)
              .asScala) { _ =>
            whenReady(saveSnapshotItemToDynamo(nextPid.id, item))(_ => ())
          }
        }
      }

      val stateProbe = createTestProbe[String]()
      persister ! GetState(stateProbe.ref)
      persister ! Stop(probe.ref)
      probe.expectMessage(Done)
      val firstState = stateProbe.receiveMessage()

      val secondEntity = spawn(Persister(nextPid))
      secondEntity ! GetState(stateProbe.ref)
      val secondState = stateProbe.receiveMessage()

      secondState shouldBe firstState
    }

    s"write events larger than the threshold ($s3Threshold) to S3" in new Setup {
      val padding = {
        var base = Iterator
          .continually(Random.nextPrintableChar())
          .take(2 * s3Threshold.toInt)
          .mkString("")
          .replaceAll("snap", "")

        while (base.length < s3Threshold.toInt) { base = base + base }

        base
      }

      persister ! PersistWithAck(padding, probe.ref)
      probe.expectMessage(Done)

      whenReady(
        minioClient
          .getObject(
            GetObjectArgs.builder
              .bucket(eventBucket)
              .`object`(s"${persistenceId.entityTypeHint}/${persistenceId.entityId}/0/1")
              .build)
          .asScala) { response =>
        val size = Source.fromInputStream(response).size
        size shouldBe padding.length

        whenReady(getEventItemFromDynamo(persistenceId.id, 1)) { item =>
          import JournalAttributes._

          item.keySet.intersect(Set(EventSerId, EventSerManifest, EventPayload)) shouldBe empty
          item.keySet.intersect(Set(BreadcrumbSerId, BreadcrumbSerManifest, BreadcrumbPayload)).size shouldBe 3
        }
      }

      persister ! Stop(probe.ref)
      probe.expectMessage(Done)

      val reincarnation = spawn(Persister(persistenceId))
      val stateProbe = createTestProbe[String]()
      reincarnation ! GetState(stateProbe.ref)
      stateProbe.expectMessage(padding)

      // This is technically the same thing as reincarnation, but to be clear that events by pid works
      val currentEventsByPidSubscriber =
        query.currentEventsByPersistenceIdTyped[String](persistenceId.id, 0, 1).runWith(sinkProbe)

      val cebpProbe = currentEventsByPidSubscriber.request(1)
      val cebpEvent = cebpProbe.expectNext()

      cebpEvent.getEvent() shouldBe padding

      cebpProbe.cancel()

      val currentEventsBySlicesSubscriber = query
        .currentEventsBySlices[String](entityType, slice, slice, TimestampOffset.Zero)
        .filter(_.persistenceId == persistenceId.id)
        .runWith(sinkProbe)

      val cebsProbe = currentEventsBySlicesSubscriber.request(1)
      val cebsEvent = cebsProbe.expectNext()

      cebsEvent.getEvent() shouldBe padding

      cebsProbe.cancel()

      // We want to force backtracking to see the first event, so persist another
      //reincarnation ! PersistWithAck("ignore-me", probe.ref)
      //probe.expectMessage(Done)

      val liveEventsBySlicesSubscriber = query
        .eventsBySlices[String](entityType, slice, slice, TimestampOffset.Zero)
        .filter(_.persistenceId == persistenceId.id)
        .runWith(sinkProbe)

      val lebsProbe = liveEventsBySlicesSubscriber.request(2)
      val forwardLebsEvent = lebsProbe.expectNext()

      forwardLebsEvent.event shouldBe padding

      val backtrackLebsEvent = lebsProbe.expectNext()

      backtrackLebsEvent.eventOption shouldBe empty
      backtrackLebsEvent.source shouldBe EnvelopeOrigin.SourceBacktracking

      lebsProbe.cancel()
    }
  }

  def getSnapshotItemFromDynamo(pid: String): Future[Map[String, AttributeValue]] = {
    ddbClient
      .getItem(
        GetItemRequest.builder
          .tableName(settings.snapshotTable)
          .consistentRead(true)
          .key(Map(SnapshotAttributes.Pid -> AttributeValue.fromS(pid)).asJava)
          .build)
      .asScala
      .map { response =>
        val item = response.item.asScala.toMap
        item shouldNot be(empty)
        item
      }(typedSystem.executionContext)
  }

  def saveSnapshotItemToDynamo(pid: String, item: Map[String, AttributeValue]): Future[Done] = {
    val itemToSave = item.updated(SnapshotAttributes.Pid, AttributeValue.fromS(pid))
    ddbClient
      .putItem(PutItemRequest.builder.tableName(settings.snapshotTable).item(itemToSave.asJava).build)
      .asScala
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  def getEventItemFromDynamo(pid: String, seqNr: Long): Future[Map[String, AttributeValue]] = {
    ddbClient
      .getItem(
        GetItemRequest.builder
          .tableName(settings.journalTable)
          .consistentRead(true)
          .key(Map(
            JournalAttributes.Pid -> AttributeValue.fromS(pid),
            JournalAttributes.SeqNr -> AttributeValue.fromN(seqNr.toString)).asJava)
          .build)
      .asScala
      .map { response =>
        val item = response.item.asScala.toMap
        item shouldNot be(empty)
        item
      }(typedSystem.executionContext)
  }
}
