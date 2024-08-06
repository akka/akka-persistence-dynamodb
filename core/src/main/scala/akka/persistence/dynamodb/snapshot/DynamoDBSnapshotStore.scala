/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.snapshot

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.persistence.SelectedSnapshot
import akka.persistence.SnapshotMetadata
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.QueryDao
import akka.persistence.dynamodb.internal.SerializedJournalItem
import akka.persistence.dynamodb.internal.SerializedSnapshotItem
import akka.persistence.dynamodb.internal.SerializedSnapshotMetadata
import akka.persistence.dynamodb.internal.SnapshotDao
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.typed.PersistenceId
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] object DynamoDBSnapshotStore {
  private def deserializeSnapshotItem(snap: SerializedSnapshotItem, serialization: Serialization): SelectedSnapshot =
    SelectedSnapshot(
      SnapshotMetadata(
        snap.persistenceId,
        snap.seqNr,
        snap.writeTimestamp.toEpochMilli,
        snap.metadata.map(serializedMeta =>
          serialization
            .deserialize(serializedMeta.payload, serializedMeta.serId, serializedMeta.serManifest)
            .get)),
      serialization.deserialize(snap.payload, snap.serId, snap.serManifest).get)
}

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] final class DynamoDBSnapshotStore(cfg: Config, cfgPath: String) extends SnapshotStore {
  import DynamoDBSnapshotStore._

  implicit val system: ActorSystem[_] = context.system.toTyped
  implicit val ec: ExecutionContext = context.dispatcher

  private val sharedConfigPath = cfgPath.replaceAll("""\.snapshot$""", "")
  private val serialization: Serialization = SerializationExtension(context.system)
  private val settings = DynamoDBSettings(context.system.settings.config.getConfig(sharedConfigPath))
  log.debug("DynamoDB snapshot store starting up")

  private val client = ClientProvider(system).clientFor(sharedConfigPath + ".client")
  private val snapshotDao = new SnapshotDao(system, settings, client)
  private val queryDao = new QueryDao(system, settings, client)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    snapshotDao
      .load(persistenceId, criteria)
      .map(_.map(item => deserializeSnapshotItem(item, serialization)))
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val snapshotAnyRef = snapshot.asInstanceOf[AnyRef]
    val serializedSnapshot = serialization.serialize(snapshotAnyRef).get
    val snapshotSerializer = serialization.findSerializerFor(snapshotAnyRef)
    val snapshotManifest = Serializers.manifestFor(snapshotSerializer, snapshotAnyRef)

    val serializedMeta: Option[SerializedSnapshotMetadata] = metadata.metadata.map { meta =>
      val metaAnyRef = meta.asInstanceOf[AnyRef]
      val serializedMeta = serialization.serialize(metaAnyRef).get
      val metaSerializer = serialization.findSerializerFor(metaAnyRef)
      val metaManifest = Serializers.manifestFor(metaSerializer, metaAnyRef)
      SerializedSnapshotMetadata(metaSerializer.identifier, metaManifest, serializedMeta)
    }

    val correspondingEvent: Future[Option[SerializedJournalItem]] =
      if (settings.querySettings.startFromSnapshotEnabled)
        queryDao.loadEvent(metadata.persistenceId, metadata.sequenceNr, includePayload = false)
      else
        Future.successful(None)

    // use same timestamp and tags as the corresponding event, if startFromSnapshotEnabled and event exists
    correspondingEvent.flatMap { eventItemOpt =>
      val writeTimestamp = Instant.ofEpochMilli(metadata.timestamp)

      val (eventTimestamp, tags) = eventItemOpt match {
        case Some(eventItem) => (eventItem.writeTimestamp, eventItem.tags)
        case None            => (writeTimestamp, Set.empty[String])
      }

      val snapshotItem = SerializedSnapshotItem(
        metadata.persistenceId,
        metadata.sequenceNr,
        writeTimestamp,
        eventTimestamp,
        serializedSnapshot,
        snapshotSerializer.identifier,
        snapshotManifest,
        tags,
        serializedMeta)

      snapshotDao.store(snapshotItem)
    }
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val criteria =
      if (metadata.timestamp == 0L)
        SnapshotSelectionCriteria(maxSequenceNr = metadata.sequenceNr, minSequenceNr = metadata.sequenceNr)
      else
        SnapshotSelectionCriteria(
          maxSequenceNr = metadata.sequenceNr,
          minSequenceNr = metadata.sequenceNr,
          maxTimestamp = metadata.timestamp,
          minTimestamp = metadata.timestamp)
    deleteAsync(metadata.persistenceId, criteria)
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)
    timeToLiveSettings.useTimeToLiveForDeletes match {
      case Some(timeToLive) =>
        val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
        log.debug(
          "deleting snapshot with time-to-live for persistence id [{}], with criteria [{}], expiring at [{}]",
          persistenceId,
          criteria,
          expiryTimestamp)
        snapshotDao.updateExpiry(persistenceId, criteria, expiryTimestamp)
      case None =>
        log.debug("deleting snapshot for persistence id [{}], with criteria [{}]", persistenceId, criteria)
        snapshotDao.delete(persistenceId, criteria)
    }
  }
}
