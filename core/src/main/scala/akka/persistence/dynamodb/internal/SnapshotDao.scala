/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant
import java.util.concurrent.CompletionException
import java.util.{ HashMap => JHashMap }
import java.util.{ Map => JMap }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.scaladsl.Source
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SnapshotDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[SnapshotDao])
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SnapshotDao(
    system: ActorSystem[_],
    settings: DynamoDBSettings,
    client: DynamoDbAsyncClient,
    s3Fallback: Option[S3Fallback])
    extends BySliceQuery.Dao[SerializedSnapshotItem] {
  import SnapshotDao._

  private val persistenceExt: Persistence = Persistence(system)

  private implicit val ec: ExecutionContext = system.executionContext

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotItem]] = {
    import SnapshotAttributes._

    val (filter, attributes) = criteriaCondition(criteria)

    attributes.put(":pid", AttributeValue.fromS(persistenceId))

    val request = {
      val builder = QueryRequest
        .builder()
        .tableName(settings.snapshotTable)
        .consistentRead(true)
        .keyConditionExpression(s"$Pid = :pid")
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .expressionAttributeValues(attributes)
      if (filter.nonEmpty) {
        builder.filterExpression(filter).build()
      } else {
        builder.build()
      }
    }

    val entityType = PersistenceId.extractEntityType(persistenceId)
    val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)

    client
      .query(request)
      .asScala
      .flatMap { response =>
        val items = response.items()
        if (items.isEmpty) {
          Future.successful(None)
        } else {
          val item = response.items.get(0)
          if (timeToLiveSettings.checkExpiry && itemHasExpired(item)) {
            Future.successful(None)
          } else {
            val snapshot = createSerializedSnapshotItem(item)

            if (s3Fallback.isEmpty || snapshot.serManifest != S3FallbackSerializer.BreadcrumbManifest) {
              log.debug(
                "Loaded snapshot for persistenceId [{}], consumed [{}] RCU",
                persistenceId,
                response.consumedCapacity.capacityUnits)

              Future.successful(Some(snapshot))
            } else {
              val serialization = SerializationExtension(system)

              val bucket = serialization.deserialize(snapshot.payload, snapshot.serId, snapshot.serManifest).get match {
                case S3Breadcrumb(bucket) => bucket
                case o =>
                  log.error(
                    "Tried to decode apparent S3Breadcrumb (snapshot) that wasn't (persistence ID [{}], seqNr [{}])",
                    snapshot.persistenceId,
                    snapshot.seqNr)
                  throw new scala.MatchError(o)
              }

              s3Fallback.get
                .loadSnapshot(snapshot.persistenceId, bucket)
                .map { maybeFromS3 =>
                  // It's possible that a new snapshot has been written since the breadcrumb, so we need to
                  // check against the criteria
                  maybeFromS3.flatMap { fromS3 =>
                    if ((fromS3.seqNr <= criteria.maxSequenceNr) &&
                      (fromS3.seqNr >= criteria.minSequenceNr) &&
                      (fromS3.writeTimestamp.compareTo(InstantFactory.fromEpochMicros(criteria.maxTimestamp)) <= 0) &&
                      (fromS3.writeTimestamp.compareTo(InstantFactory.fromEpochMicros(criteria.minTimestamp)) >= 0)) {
                      Some(SerializedSnapshotItem(
                        persistenceId = snapshot.persistenceId,
                        seqNr = fromS3.seqNr,
                        writeTimestamp = fromS3.writeTimestamp,
                        eventTimestamp = fromS3.eventTimestamp,
                        payload = fromS3.payload,
                        serId = fromS3.serId,
                        serManifest = fromS3.serManifest,
                        tags = fromS3.tags,
                        metadata = fromS3.metadata))
                    } else None
                  }
                }(ExecutionContext.parasitic)
            }
          }
        }
      }
      .recoverWith { case c: CompletionException =>
        Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
  }

  private def itemHasExpired(item: JMap[String, AttributeValue]): Boolean = {
    import SnapshotAttributes.Expiry
    if (item.containsKey(Expiry)) {
      val now = System.currentTimeMillis / 1000
      item.get(Expiry).n.toLong <= now
    } else false
  }

  def store(snapshot: SerializedSnapshotItem): Future[Unit] = {
    import SnapshotAttributes._

    if (s3Fallback.nonEmpty &&
      snapshot.payload.length > settings.s3FallbackSettings.threshold &&
      snapshot.serManifest != S3FallbackSerializer.BreadcrumbManifest) {
      s3Fallback.get.saveSnapshot(snapshot).flatMap { _ =>
        val breadcrumb = S3Breadcrumb(settings.s3FallbackSettings.snapshotsBucket)
        val serialization = SerializationExtension(system)

        val bytes = serialization.serialize(breadcrumb).get
        val serializer = serialization.findSerializerFor(breadcrumb)
        val manifest = Serializers.manifestFor(serializer, breadcrumb)

        store(
          SerializedSnapshotItem(
            persistenceId = snapshot.persistenceId,
            seqNr = snapshot.seqNr,
            writeTimestamp = snapshot.writeTimestamp,
            eventTimestamp = snapshot.eventTimestamp,
            payload = bytes,
            serId = serializer.identifier,
            serManifest = manifest,
            tags = snapshot.tags,
            metadata = snapshot.metadata))
      }
    } else {
      val persistenceId = snapshot.persistenceId
      val entityType = PersistenceId.extractEntityType(persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)
      val eventTimestampMicros = InstantFactory.toEpochMicros(snapshot.readTimestamp)

      val attributes = new JHashMap[String, AttributeValue]
      attributes.put(Pid, AttributeValue.fromS(persistenceId))
      attributes.put(SeqNr, AttributeValue.fromN(snapshot.seqNr.toString))
      attributes.put(EntityTypeSlice, AttributeValue.fromS(s"$entityType-$slice"))
      attributes.put(WriteTimestamp, AttributeValue.fromN(snapshot.writeTimestamp.toEpochMilli.toString))
      attributes.put(EventTimestamp, AttributeValue.fromN(eventTimestampMicros.toString))
      attributes.put(SnapshotSerId, AttributeValue.fromN(snapshot.serId.toString))
      attributes.put(SnapshotSerManifest, AttributeValue.fromS(snapshot.serManifest))
      attributes.put(SnapshotPayload, AttributeValue.fromB(SdkBytes.fromByteArray(snapshot.payload)))

      if (snapshot.tags.nonEmpty) { // note: DynamoDB does not support empty sets
        attributes.put(Tags, AttributeValue.fromSs(snapshot.tags.toSeq.asJava))
      }

      snapshot.metadata.foreach { meta =>
        attributes.put(MetaSerId, AttributeValue.fromN(meta.serId.toString))
        attributes.put(MetaSerManifest, AttributeValue.fromS(meta.serManifest))
        attributes.put(MetaPayload, AttributeValue.fromB(SdkBytes.fromByteArray(meta.payload)))
      }

      val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)

      timeToLiveSettings.snapshotTimeToLive.foreach { timeToLive =>
        val expiryTimestamp = snapshot.writeTimestamp.plusSeconds(timeToLive.toSeconds)
        attributes.put(Expiry, AttributeValue.fromN(expiryTimestamp.getEpochSecond.toString))
      }

      val request = PutItemRequest
        .builder()
        .tableName(settings.snapshotTable)
        .item(attributes)
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .build()

      val result = client.putItem(request).asScala

      if (log.isDebugEnabled()) {
        result.foreach { response =>
          log.debug(
            "Stored snapshot for persistenceId [{}], consumed [{}] WCU",
            persistenceId,
            response.consumedCapacity.capacityUnits)
        }
      }

      result
        .map(_ => ())(ExecutionContext.parasitic)
        .recoverWith { case c: CompletionException =>
          Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
    }
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    import SnapshotAttributes._

    // FIXME: support S3 fallback

    val (condition, attributes) = criteriaCondition(criteria)

    val request = {
      val builder = DeleteItemRequest
        .builder()
        .tableName(settings.snapshotTable)
        .key(Map(Pid -> AttributeValue.fromS(persistenceId)).asJava)
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      if (condition.nonEmpty) {
        builder
          .conditionExpression(condition)
          .expressionAttributeValues(attributes)
          .build()
      } else {
        builder.build()
      }
    }

    val result = client.deleteItem(request).asScala

    if (log.isDebugEnabled()) {
      result.foreach { response =>
        log.debug(
          "Deleted snapshot for persistenceId [{}], consumed [{}] WCU",
          persistenceId,
          response.consumedCapacity.capacityUnits)
      }
    }

    result
      .map(_ => ())(ExecutionContext.parasitic)
      // ignore if the criteria conditional check failed
      .recover {
        case _: ConditionalCheckFailedException => ()
        case e: CompletionException =>
          e.getCause match {
            case _: ConditionalCheckFailedException => ()
            case cause                              => throw cause
          }
      }(ExecutionContext.parasitic)
  }

  def updateExpiry(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria,
      expiryTimestamp: Instant): Future[Unit] = {
    import SnapshotAttributes._

    val (condition, attributes) = criteriaCondition(criteria)

    attributes.put(":expiry", AttributeValue.fromN(expiryTimestamp.getEpochSecond.toString))

    val request = {
      val builder = UpdateItemRequest.builder
        .tableName(settings.snapshotTable)
        .key(Map(Pid -> AttributeValue.fromS(persistenceId)).asJava)
        .updateExpression(s"SET $Expiry = :expiry")
        .expressionAttributeValues(attributes)
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      if (condition.nonEmpty) {
        builder
          .conditionExpression(condition)
          .build()
      } else {
        builder.build()
      }
    }

    val result = client.updateItem(request).asScala

    if (log.isDebugEnabled()) {
      result.foreach { response =>
        log.debug(
          "Updated expiry of snapshot for persistenceId [{}], expiring at [{}], consumed [{}] WCU",
          persistenceId,
          expiryTimestamp,
          response.consumedCapacity.capacityUnits)
      }
    }

    result
      .map(_ => ())(ExecutionContext.parasitic)
      // ignore if the criteria conditional check failed
      .recover {
        case _: ConditionalCheckFailedException => ()
        case e: CompletionException =>
          e.getCause match {
            case _: ConditionalCheckFailedException => ()
            case cause                              => throw cause
          }
      }(ExecutionContext.parasitic)
  }

  // Used from `BySliceQuery` (only if settings.querySettings.startFromSnapshotEnabled).
  override def itemsBySlice(
      entityType: String,
      slice: Int,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      backtracking: Boolean): Source[SerializedSnapshotItem, NotUsed] = {
    import SnapshotAttributes._

    val entityTypeSlice = s"$entityType-$slice"

    val attributeValues =
      Map(
        ":entityTypeSlice" -> AttributeValue.fromS(entityTypeSlice),
        ":from" -> AttributeValue.fromN(InstantFactory.toEpochMicros(fromTimestamp).toString),
        ":to" -> AttributeValue.fromN(InstantFactory.toEpochMicros(toTimestamp).toString))

    val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)

    val (filterExpression, filterAttributeValues) =
      if (timeToLiveSettings.checkExpiry) {
        val now = System.currentTimeMillis / 1000
        val expression = s"attribute_not_exists($Expiry) OR $Expiry > :now"
        val attributes = Map(":now" -> AttributeValue.fromN(now.toString))
        (Some(expression), attributes)
      } else {
        (None, Map.empty[String, AttributeValue])
      }

    val requestBuilder = QueryRequest.builder
      .tableName(settings.snapshotTable)
      .indexName(settings.snapshotBySliceGsi)
      .keyConditionExpression(s"$EntityTypeSlice = :entityTypeSlice AND $EventTimestamp BETWEEN :from AND :to")
      .expressionAttributeValues((attributeValues ++ filterAttributeValues).asJava)
      // Limit won't limit the number of results you get with the paginator.
      // It only limits the number of results in each page.
      // See the `take` below which limits the total number of results.
      // Limit is ignored by local DynamoDB.
      .limit(settings.querySettings.bufferSize)

    filterExpression.foreach(requestBuilder.filterExpression)

    val publisher = client.queryPaginator(requestBuilder.build())

    Source
      .fromPublisher(publisher)
      .mapConcat(_.items.iterator.asScala)
      .take(settings.querySettings.bufferSize)
      .map(createSerializedSnapshotItem)
      .mapError { case c: CompletionException =>
        c.getCause
      }
  }

  private def createSerializedSnapshotItem(item: JMap[String, AttributeValue]): SerializedSnapshotItem = {
    import SnapshotAttributes._

    val metadata = Option(item.get(MetaPayload)).map { metaPayload =>
      SerializedSnapshotMetadata(
        serId = item.get(MetaSerId).n().toInt,
        serManifest = item.get(MetaSerManifest).s(),
        payload = metaPayload.b().asByteArray())
    }

    SerializedSnapshotItem(
      persistenceId = item.get(Pid).s(),
      seqNr = item.get(SeqNr).n().toLong,
      writeTimestamp = Instant.ofEpochMilli(item.get(WriteTimestamp).n().toLong),
      eventTimestamp = InstantFactory.fromEpochMicros(item.get(EventTimestamp).n().toLong),
      payload = item.get(SnapshotPayload).b().asByteArray(),
      serId = item.get(SnapshotSerId).n().toInt,
      serManifest = item.get(SnapshotSerManifest).s(),
      tags = if (item.containsKey(Tags)) item.get(Tags).ss().asScala.toSet else Set.empty,
      metadata = metadata)
  }

  // optional condition expression and attribute values, based on selection criteria
  private def criteriaCondition(criteria: SnapshotSelectionCriteria): (String, JHashMap[String, AttributeValue]) = {
    import SnapshotAttributes._

    val conditions = Seq.newBuilder[String]
    val attributes = new JHashMap[String, AttributeValue]

    if (criteria.maxSequenceNr != Long.MaxValue) {
      conditions += s"$SeqNr <= :maxSeqNr"
      attributes.put(":maxSeqNr", AttributeValue.fromN(criteria.maxSequenceNr.toString))
    }

    if (criteria.minSequenceNr > 0L) {
      conditions += s"$SeqNr >= :minSeqNr"
      attributes.put(":minSeqNr", AttributeValue.fromN(criteria.minSequenceNr.toString))
    }

    if (criteria.maxTimestamp != Long.MaxValue) {
      conditions += s"$WriteTimestamp <= :maxWriteTimestamp"
      attributes.put(":maxWriteTimestamp", AttributeValue.fromN(criteria.maxTimestamp.toString))
    }

    if (criteria.minTimestamp != 0L) {
      conditions += s"$WriteTimestamp >= :minWriteTimestamp"
      attributes.put(":minWriteTimestamp", AttributeValue.fromN(criteria.minTimestamp.toString))
    }

    (conditions.result().mkString(" AND "), attributes)
  }

}
