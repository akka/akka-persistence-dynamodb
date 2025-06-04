/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.dynamodb.internal.S3Fallback
import akka.persistence.dynamodb.internal.SnapshotDao
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.typed.EventEnvelope
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import io.minio.GetObjectArgs

import scala.io.Source
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Random
import io.minio.CopyObjectArgs
import io.minio.CopySource
import akka.persistence.dynamodb.internal.QueryDao
import akka.persistence.query.TimestampOffset

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

  val s3Threshold = system.settings.config.getBytes("akka.persistence.dynamodb.fallback-to-s3.threshold")
  val snapshotBucket = system.settings.config.getString("akka.persistence.dynamodb.fallback-to-s3.snapshots-bucket")
  val eventBucket = system.settings.config.getString("akka.persistence.dynamodb.fallback-to-s3.events-bucket")

  val disabledFallbackConfig =
    ConfigFactory
      .parseString("akka.persistence.dynamodb.fallback-to-s3.enabled = off")
      .withFallback(system.settings.config)
      .resolve

  val ddbSettingsWithDisabledFallback = DynamoDBSettings(disabledFallbackConfig.getConfig("akka.persistence.dynamodb"))

  val ddbClient = ClientProvider(system).clientFor("akka.persistence.dynamodb.client")
  val ddbClientSettings = ClientProvider(system).clientSettingsFor("akka.persistence.dynamodb.client")
  val noFallbackSnapshotDao = new SnapshotDao(
    system,
    ddbSettingsWithDisabledFallback,
    ddbClient,
    S3Fallback(ddbSettingsWithDisabledFallback, ddbClientSettings, system))

  val noFallbackQueryDao = new QueryDao(
    system,
    ddbSettingsWithDisabledFallback,
    ddbClient,
    S3Fallback(ddbSettingsWithDisabledFallback, ddbClientSettings, system))

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

        whenReady(noFallbackSnapshotDao.load(persistenceId.id, SnapshotSelectionCriteria.Latest)) { snapResult =>
          snapResult shouldNot be(empty)
          snapResult.get.serManifest shouldBe "akka.persistence.dynamodb.S3Breadcrumb"
          assert(
            snapResult.get.payload.length < size,
            "Snapshot breadcrumb in dynamo should be smaller than snapshot in S3")

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
            noFallbackSnapshotDao.store(snapResult.get.copy(persistenceId = nextPid.id))
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
      val padding =
        Iterator.continually(Random.nextPrintableChar()).take(2 * s3Threshold.toInt).mkString("").replaceAll("snap", "")

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

        whenReady(noFallbackQueryDao.loadEvent(persistenceId.id, 1, true)) { evtResult =>
          evtResult shouldNot be(empty)
          evtResult.get.serManifest shouldBe "akka.persistence.dynamodb.S3Breadcrumb"
          assert(
            evtResult.get.payload.get.length < size,
            "Event breadcrumb in Dynamo should be smaller than event in S3")
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
    }
  }
}
