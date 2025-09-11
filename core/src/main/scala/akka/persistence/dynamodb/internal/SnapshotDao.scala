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
import scala.util.Failure
import scala.util.Success

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.EventSourcedEntityTimeToLiveSettings
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
    client: DynamoDbAsyncClient)
    extends BySliceQuery.Dao[SerializedSnapshotItem] {
  import SnapshotDao._

  private val persistenceExt: Persistence = Persistence(system)
  private val serialization = SerializationExtension(system)
  private val fallbackStoreProvider = FallbackStoreProvider(system)

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
            createSnapshotItem(item) match {
              case snapshot: SerializedSnapshotItem =>
                log.debug(
                  "Loaded snapshot for persistenceId [{}], consumed [{}] RCU",
                  persistenceId,
                  response.consumedCapacity.capacityUnits)

                Future.successful(Some(snapshot))

              case SnapshotItemWithBreadcrumb(persistenceId, seqNr, breadcrumb) =>
                if (!settings.snapshotFallbackSettings.isEnabled) {
                  val msg =
                    "Failed to follow breadcrumb to snapshot in fallback store because fallback was " +
                    s"disabled: persistenceId [$persistenceId] seqNr [$seqNr]"
                  log.error(msg)
                  Future.failed(new NoSuchElementException(msg))
                } else {
                  val fallbackStore =
                    fallbackStoreProvider.snapshotFallbackStoreFor(settings.snapshotFallbackSettings.plugin)
                  serialization.deserialize(breadcrumb._3, breadcrumb._1, breadcrumb._2) match {
                    case Success(candidate) =>
                      fallbackStore.toBreadcrumb(candidate) match {
                        case Some(breadcrumb) =>
                          fallbackStore
                            .loadSnapshot(breadcrumb, persistenceId, seqNr)
                            .map { maybeFromFallback =>
                              // The breadcrumb met the snapshot selection criteria, but it's possible that a
                              // snapshot has been saved after the breadcrumb, so we need to recheck
                              maybeFromFallback.flatMap { fromFallback =>
                                if ((fromFallback.seqNr <= criteria.maxSequenceNr) &&
                                  (fromFallback.seqNr >= criteria.minSequenceNr) &&
                                  (fromFallback.writeTimestamp.compareTo(
                                    InstantFactory.fromEpochMicros(criteria.maxTimestamp)) <= 0) &&
                                  (fromFallback.writeTimestamp.compareTo(
                                    InstantFactory.fromEpochMicros(criteria.minTimestamp)) >= 0)) {

                                  // snapshot from fallback meets criteria

                                  val metadata = fromFallback.meta.map { case ((serId, manifest), payload) =>
                                    SerializedSnapshotMetadata(serId, manifest, payload)
                                  }

                                  Some(SerializedSnapshotItem(
                                    persistenceId = persistenceId,
                                    seqNr = fromFallback.seqNr,
                                    writeTimestamp = fromFallback.writeTimestamp,
                                    eventTimestamp = fromFallback.eventTimestamp,
                                    payload = fromFallback.payload,
                                    serId = fromFallback.serId,
                                    serManifest = fromFallback.serManifest,
                                    tags = fromFallback.tags,
                                    metadata = metadata))
                                } else None
                              }
                            }

                        case None =>
                          val msg =
                            s"Breadcrumb rejected by fallback store for snapshot at persistenceId [$persistenceId] seqNr [$seqNr]"
                          log.error(msg)
                          Future.failed(new IllegalStateException(msg))
                      }

                    case Failure(ex) =>
                      val msg =
                        s"Failed to deserialize breadcrumb for snapshot at persistenceId [$persistenceId] seqNr [$seqNr]"
                      log.error(msg, ex)
                      Future.failed(ex)
                  }
                }
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
    val persistenceId = snapshot.persistenceId
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val eventTimestampMicros = InstantFactory.toEpochMicros(snapshot.readTimestamp)
    val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)

    if (settings.snapshotFallbackSettings.isEnabled) {
      if (log.isDebugEnabled) {
        log.debug("Fallback store will be used")
      }

      val size = snapshot.estimatedDynamoSize(entityType, timeToLiveSettings.snapshotTimeToLive.isDefined)

      if (size >= settings.snapshotFallbackSettings.threshold) {

        val fallbackStore = fallbackStoreProvider.snapshotFallbackStoreFor(settings.snapshotFallbackSettings.plugin)

        val meta = snapshot.metadata.map { m => (m.serId -> m.serManifest) -> m.payload }

        fallbackStore
          .saveSnapshot(
            persistenceId,
            snapshot.seqNr,
            snapshot.writeTimestamp,
            snapshot.eventTimestamp,
            snapshot.serId,
            snapshot.serManifest,
            snapshot.payload,
            snapshot.tags,
            meta)
          .flatMap { breadcrumb =>
            if (log.isDebugEnabled) {
              log.debug("Saved snapshot for persistenceId [{}] to fallback store", persistenceId)
            }

            val bytes = serialization.serialize(breadcrumb).get
            val serializer = serialization.findSerializerFor(breadcrumb)
            val manifest = Serializers.manifestFor(serializer, breadcrumb)

            saveItemWithBreadcrumb(
              SnapshotItemWithBreadcrumb(persistenceId, snapshot.seqNr, (serializer.identifier, manifest, bytes)),
              snapshot.writeTimestamp,
              entityType,
              slice,
              timeToLiveSettings)
          }
      } else {
        saveSnapshotItem(snapshot, entityType, slice, eventTimestampMicros, timeToLiveSettings)
      }
    } else {
      saveSnapshotItem(snapshot, entityType, slice, eventTimestampMicros, timeToLiveSettings)
    }
  }

  private def saveSnapshotItem(
      snapshot: SerializedSnapshotItem,
      entityType: String,
      slice: Int,
      eventTimestampMicros: Long,
      timeToLiveSettings: EventSourcedEntityTimeToLiveSettings): Future[Unit] = {
    import SnapshotAttributes._

    val attributes = new JHashMap[String, AttributeValue]
    attributes.put(Pid, AttributeValue.fromS(snapshot.persistenceId))
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
          snapshot.persistenceId,
          response.consumedCapacity.capacityUnits)
      }
    }

    result
      .map(_ => ())(ExecutionContext.parasitic)
      .recoverWith { case c: CompletionException =>
        Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
  }

  private def saveItemWithBreadcrumb(
      withBreadcrumb: SnapshotItemWithBreadcrumb,
      timestamp: Instant,
      entityType: String,
      slice: Int,
      timeToLiveSettings: EventSourcedEntityTimeToLiveSettings): Future[Unit] = {
    import SnapshotAttributes._

    val attributes = new JHashMap[String, AttributeValue]
    attributes.put(Pid, AttributeValue.fromS(withBreadcrumb.persistenceId))
    attributes.put(SeqNr, AttributeValue.fromN(withBreadcrumb.seqNr.toString))
    attributes.put(EntityTypeSlice, AttributeValue.fromS(s"$entityType-$slice"))
    attributes.put(BreadcrumbSerId, AttributeValue.fromN(withBreadcrumb.breadcrumb._1.toString))
    attributes.put(BreadcrumbSerManifest, AttributeValue.fromS(withBreadcrumb.breadcrumb._2))
    attributes.put(BreadcrumbPayload, AttributeValue.fromB(SdkBytes.fromByteArray(withBreadcrumb.breadcrumbPayload)))

    timeToLiveSettings.snapshotTimeToLive.foreach { timeToLive =>
      val expiryTimestamp = timestamp.plusSeconds(timeToLive.toSeconds)
      attributes.put(Expiry, AttributeValue.fromN(expiryTimestamp.getEpochSecond.toString))
    }

    val result = client
      .putItem(
        PutItemRequest.builder
          .tableName(settings.snapshotTable)
          .item(attributes)
          .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
          .build)
      .asScala

    if (log.isDebugEnabled) {
      result.foreach { response =>
        log.debug(
          "Stored snapshot for persistenceId [{}], consumed [{}] WCU",
          withBreadcrumb.persistenceId,
          response.consumedCapacity)
      }
    }

    result
      .map(_ => ())(ExecutionContext.parasitic)
      .recoverWith { case c: CompletionException =>
        Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
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

  private def createSnapshotItem(item: JMap[String, AttributeValue]): ItemInSnapshotStore = {
    import SnapshotAttributes._

    val persistenceId = item.get(Pid).s
    val seqNr = item.get(SeqNr).n.toLong

    if (item.containsKey(BreadcrumbSerId)) {
      val breadcrumb = (
        item.get(BreadcrumbSerId).n.toInt,
        item.get(BreadcrumbSerManifest).s,
        item.get(BreadcrumbPayload).b.asByteArray)
      SnapshotItemWithBreadcrumb(persistenceId = persistenceId, seqNr = seqNr, breadcrumb = breadcrumb)
    } else {
      val metadata = Option(item.get(MetaPayload)).map { metaPayload =>
        SerializedSnapshotMetadata(
          serId = item.get(MetaSerId).n().toInt,
          serManifest = item.get(MetaSerManifest).s(),
          payload = metaPayload.b().asByteArray())
      }

      SerializedSnapshotItem(
        persistenceId = persistenceId,
        seqNr = seqNr,
        writeTimestamp = Instant.ofEpochMilli(item.get(WriteTimestamp).n().toLong),
        eventTimestamp = InstantFactory.fromEpochMicros(item.get(EventTimestamp).n().toLong),
        payload = item.get(SnapshotPayload).b().asByteArray(),
        serId = item.get(SnapshotSerId).n().toInt,
        serManifest = item.get(SnapshotSerManifest).s(),
        tags = if (item.containsKey(Tags)) item.get(Tags).ss().asScala.toSet else Set.empty,
        metadata = metadata)
    }
  }

  private def createSerializedSnapshotItem(item: JMap[String, AttributeValue]): SerializedSnapshotItem =
    createSnapshotItem(item).asInstanceOf[SerializedSnapshotItem]

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

  if (settings.snapshotFallbackSettings.isEnabled && settings.snapshotFallbackSettings.eager) {
    // constructs and saves in a concurrent hashmap for later use
    fallbackStoreProvider.snapshotFallbackStoreFor(settings.snapshotFallbackSettings.plugin)
  }
}
