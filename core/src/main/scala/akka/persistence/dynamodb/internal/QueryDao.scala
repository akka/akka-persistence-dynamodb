/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant
import java.util.concurrent.CompletionException
import java.util.{ Map => JMap }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.typed.PersistenceId
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import akka.serialization.SerializationExtension

/**
 * INTERNAL API
 */
@InternalApi private[akka] class QueryDao(
    system: ActorSystem[_],
    settings: DynamoDBSettings,
    client: DynamoDbAsyncClient,
    s3Fallback: Option[S3Fallback])
    extends BySliceQuery.Dao[SerializedJournalItem] {
  import system.executionContext

  private val bySliceProjectionExpression = {
    import JournalAttributes._
    s"$Pid, $SeqNr, $Timestamp, $EventSerId, $EventSerManifest, $Tags"
  }

  private val bySliceWithMetaProjectionExpression = {
    import JournalAttributes._
    s"$bySliceProjectionExpression, $MetaSerId, $MetaSerManifest, $MetaPayload"
  }

  private val bySliceWithPayloadProjectionExpression = {
    import JournalAttributes._
    s"$bySliceWithMetaProjectionExpression, $EventPayload"
  }

  private val logging = Logging(system.classicSystem, this.getClass.getName)

  def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      includeDeleted: Boolean): Source[SerializedJournalItem, NotUsed] = {
    import JournalAttributes._

    if (toSequenceNr < fromSequenceNr) { // when max of 0
      Source.empty
    } else {
      val attributeValues =
        Map(
          ":pid" -> AttributeValue.fromS(persistenceId),
          ":from" -> AttributeValue.fromN(fromSequenceNr.toString),
          ":to" -> AttributeValue.fromN(toSequenceNr.toString))

      val entityType = PersistenceId.extractEntityType(persistenceId)
      val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)
      val checkExpiry = timeToLiveSettings.checkExpiry
      val now = if (checkExpiry) System.currentTimeMillis / 1000 else 0

      val (filterExpression, filterAttributeValues) =
        if (checkExpiry) {
          val expression = {
            if (includeDeleted) // allow delete or expiry markers, but still filter expired events
              s"attribute_not_exists($Expiry) OR $Expiry > :now"
            else // no delete marker or expired events (checking expiry and expiry marker)
              s"attribute_not_exists($Deleted)" +
              s" AND (attribute_not_exists($Expiry) OR $Expiry > :now)" +
              s" AND (attribute_not_exists($ExpiryMarker) OR $ExpiryMarker > :now)"
          }
          val attributes = Map(":now" -> AttributeValue.fromN(now.toString))
          (Some(expression), attributes)
        } else if (!includeDeleted) {
          (Some(s"attribute_not_exists($Deleted)"), Map.empty[String, AttributeValue])
        } else (None, Map.empty[String, AttributeValue])

      val reqBuilder = QueryRequest.builder
        .tableName(settings.journalTable)
        .consistentRead(true)
        .keyConditionExpression(s"$Pid = :pid AND $SeqNr BETWEEN :from AND :to")
        .expressionAttributeValues((attributeValues ++ filterAttributeValues).asJava)
        .limit(settings.querySettings.bufferSize)

      filterExpression.foreach(reqBuilder.filterExpression)

      val publisher = client.queryPaginator(reqBuilder.build())

      Source
        .fromPublisher(publisher)
        .mapConcat { response =>
          response.items().iterator().asScala.map { item =>
            if (includeDeleted && (item.containsKey(Deleted) ||
              (checkExpiry && item.containsKey(ExpiryMarker) && item.get(ExpiryMarker).n.toLong <= now))) {
              // deleted or expired item
              SerializedJournalItem(
                persistenceId = persistenceId,
                seqNr = item.get(SeqNr).n().toLong,
                writeTimestamp = InstantFactory.fromEpochMicros(item.get(Timestamp).n().toLong),
                readTimestamp = InstantFactory.EmptyTimestamp,
                payload = None,
                serId = 0,
                serManifest = "",
                writerUuid = "",
                tags = Set.empty,
                metadata = None)

            } else {
              val metadata = Option(item.get(MetaPayload)).map { metaPayload =>
                SerializedEventMetadata(
                  serId = item.get(MetaSerId).n().toInt,
                  serManifest = item.get(MetaSerManifest).s(),
                  payload = metaPayload.b().asByteArray())
              }

              SerializedJournalItem(
                persistenceId = persistenceId,
                seqNr = item.get(SeqNr).n().toLong,
                writeTimestamp = InstantFactory.fromEpochMicros(item.get(Timestamp).n().toLong),
                readTimestamp = InstantFactory.EmptyTimestamp,
                payload = Some(item.get(EventPayload).b().asByteArray()),
                serId = item.get(EventSerId).n().toInt,
                serManifest = item.get(EventSerManifest).s(),
                writerUuid = item.get(Writer).s(),
                tags = if (item.containsKey(Tags)) item.get(Tags).ss().asScala.toSet else Set.empty,
                metadata = metadata)
            }
          }
        }
        .mapAsync(settings.s3FallbackSettings.eventBatchSize)(followBreadcrumb)
        .mapError { case c: CompletionException =>
          c.getCause
        }
    }
  }

  // implements BySliceQuery.Dao
  override def itemsBySlice(
      entityType: String,
      slice: Int,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      backtracking: Boolean): Source[SerializedJournalItem, NotUsed] = {
    import JournalAttributes._

    if (toTimestamp.isBefore(fromTimestamp)) {
      // possible with start-from-snapshot queries
      Source.empty
    } else {
      val entityTypeSlice = s"$entityType-$slice"

      // FIXME we could look into using response.lastEvaluatedKey and use that as exclusiveStartKey in query,
      // instead of the timestamp for subsequent queries. Not sure how that works with GSI where the
      // sort key isn't unique (same timestamp). If DynamoDB can keep track of the exact offset and
      // not emit duplicates would not need the seen Map and that filter.
      // Well, we still need it for the first query because we want the external offset to be TimestampOffset
      // and that can include seen Map.

      val attributeValues =
        Map(
          ":entityTypeSlice" -> AttributeValue.fromS(entityTypeSlice),
          ":from" -> AttributeValue.fromN(InstantFactory.toEpochMicros(fromTimestamp).toString),
          ":to" -> AttributeValue.fromN(InstantFactory.toEpochMicros(toTimestamp).toString))

      val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)

      val (filterExpression, filterAttributeValues) =
        if (timeToLiveSettings.checkExpiry) {
          val now = System.currentTimeMillis / 1000
          // no delete marker or expired events (checking expiry and expiry marker)
          val expression =
            s"attribute_not_exists($Deleted)" +
            s" AND (attribute_not_exists($Expiry) OR $Expiry > :now)" +
            s" AND (attribute_not_exists($ExpiryMarker) OR $ExpiryMarker > :now)"
          val attributes = Map(":now" -> AttributeValue.fromN(now.toString))
          (expression, attributes)
        } else {
          (s"attribute_not_exists($Deleted)", Map.empty[String, AttributeValue])
        }

      val projectionExpression =
        if (backtracking) bySliceProjectionExpression else bySliceWithPayloadProjectionExpression

      val req = QueryRequest.builder
        .tableName(settings.journalTable)
        .indexName(settings.journalBySliceGsi)
        .keyConditionExpression(s"$EntityTypeSlice = :entityTypeSlice AND $Timestamp BETWEEN :from AND :to")
        .filterExpression(filterExpression)
        .expressionAttributeValues((attributeValues ++ filterAttributeValues).asJava)
        .projectionExpression(projectionExpression)
        // Limit won't limit the number of results you get with the paginator.
        // It only limits the number of results in each page.
        // See the `take` below which limits the total number of results.
        // Limit is ignored by local DynamoDB.
        .limit(settings.querySettings.bufferSize)
        .build()

      val publisher = client.queryPaginator(req)

      def getTimestamp(item: JMap[String, AttributeValue]): Instant =
        InstantFactory.fromEpochMicros(item.get(Timestamp).n().toLong)

      val logName = s"[$entityType] itemsBySlice [$slice] [${if (backtracking) "backtracking" else "query"}]"

      def logQueryResponse: QueryResponse => String = response => {
        if (response.hasItems && !response.items.isEmpty) {
          val items = response.items
          val count = items.size
          val first = getTimestamp(items.get(0))
          val last = getTimestamp(items.get(items.size - 1))
          val scanned = response.scannedCount
          val hasMore = response.hasLastEvaluatedKey && !response.lastEvaluatedKey.isEmpty
          s"query response page with [$count] events between [$first - $last] (scanned [$scanned], has more [$hasMore])"
        } else "empty query response page"
      }

      Source
        .fromPublisher(publisher)
        // note that this is not logging each item, only the QueryResponse
        .log(logName, logQueryResponse)(logging)
        .withAttributes(Attributes
          .logLevels(onElement = Logging.DebugLevel, onFinish = Logging.DebugLevel, onFailure = Logging.WarningLevel))
        .mapConcat(_.items.iterator.asScala)
        .take(settings.querySettings.bufferSize)
        .map { item =>
          if (backtracking) {
            SerializedJournalItem(
              persistenceId = item.get(Pid).s(),
              seqNr = item.get(SeqNr).n().toLong,
              writeTimestamp = getTimestamp(item),
              readTimestamp = InstantFactory.now(),
              payload = None, // lazy loaded for backtracking
              serId = item.get(EventSerId).n().toInt,
              serManifest = "",
              writerUuid = "", // not need in this query
              tags = if (item.containsKey(Tags)) item.get(Tags).ss().asScala.toSet else Set.empty,
              metadata = None)
          } else {
            createSerializedJournalItem(item, includePayload = true)
          }
        }
        .mapAsync(settings.s3FallbackSettings.eventBatchSize)(followBreadcrumb)
        .mapError { case c: CompletionException =>
          c.getCause
        }
    }
  }

  private def createSerializedJournalItem(
      item: JMap[String, AttributeValue],
      includePayload: Boolean): SerializedJournalItem = {
    import JournalAttributes._

    val metadata = Option(item.get(MetaPayload)).map { metaPayload =>
      SerializedEventMetadata(
        serId = item.get(MetaSerId).n().toInt,
        serManifest = item.get(MetaSerManifest).s(),
        payload = metaPayload.b().asByteArray())
    }

    SerializedJournalItem(
      persistenceId = item.get(Pid).s(),
      seqNr = item.get(SeqNr).n().toLong,
      writeTimestamp = InstantFactory.fromEpochMicros(item.get(Timestamp).n().toLong),
      readTimestamp = InstantFactory.now(),
      payload = if (includePayload) Some(item.get(EventPayload).b().asByteArray()) else None,
      serId = item.get(EventSerId).n().toInt,
      serManifest = item.get(EventSerManifest).s(),
      writerUuid = "", // not need in this query
      tags = if (item.containsKey(Tags)) item.get(Tags).ss().asScala.toSet else Set.empty,
      metadata = metadata)
  }

  def timestampOfEvent(persistenceId: String, seqNr: Long): Future[Option[Instant]] = {
    import JournalAttributes._
    val attributeValues =
      Map(":pid" -> AttributeValue.fromS(persistenceId), ":seqNr" -> AttributeValue.fromN(seqNr.toString))

    val entityType = PersistenceId.extractEntityType(persistenceId)
    val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)

    val (filterExpression, filterAttributeValues) =
      if (timeToLiveSettings.checkExpiry) {
        val now = System.currentTimeMillis / 1000
        // no delete marker or expired events (checking expiry and expiry marker)
        val expression =
          s"attribute_not_exists($Deleted)" +
          s" AND (attribute_not_exists($Expiry) OR $Expiry > :now)" +
          s" AND (attribute_not_exists($ExpiryMarker) OR $ExpiryMarker > :now)"
        val attributes = Map(":now" -> AttributeValue.fromN(now.toString))
        (expression, attributes)
      } else {
        (s"attribute_not_exists($Deleted)", Map.empty[String, AttributeValue])
      }

    val req = QueryRequest.builder
      .tableName(settings.journalTable)
      .consistentRead(true)
      .keyConditionExpression(s"$Pid = :pid AND $SeqNr = :seqNr")
      .filterExpression(filterExpression)
      .expressionAttributeValues((attributeValues ++ filterAttributeValues).asJava)
      .projectionExpression(Timestamp)
      .build()

    client
      .query(req)
      .asScala
      .map { response =>
        val items = response.items()
        if (items.isEmpty)
          None
        else {
          val timestampMicros = items.get(0).get(Timestamp).n().toLong
          Some(InstantFactory.fromEpochMicros(timestampMicros))
        }
      }
      .recoverWith { case c: CompletionException =>
        Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
  }

  def loadEvent(persistenceId: String, seqNr: Long, includePayload: Boolean): Future[Option[SerializedJournalItem]] = {
    val queryResult = queryForEvent(persistenceId, seqNr, includePayload)

    queryResult.flatMap { maybeItem =>
      maybeItem match {
        case None       => queryResult
        case Some(item) => followBreadcrumb(item).map(Some.apply)(ExecutionContext.parasitic)
      }
    }(ExecutionContext.parasitic)
  }

  private def queryForEvent(
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[SerializedJournalItem]] = {
    import JournalAttributes._
    val attributeValues =
      Map(":pid" -> AttributeValue.fromS(persistenceId), ":seqNr" -> AttributeValue.fromN(seqNr.toString))

    val entityType = PersistenceId.extractEntityType(persistenceId)
    val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)

    val (filterExpression, filterAttributeValues) =
      if (timeToLiveSettings.checkExpiry) {
        val now = System.currentTimeMillis / 1000
        // no delete marker or expired events (checking expiry and expiry marker)
        val expression =
          s"attribute_not_exists($Deleted)" +
          s" AND (attribute_not_exists($Expiry) OR $Expiry > :now)" +
          s" AND (attribute_not_exists($ExpiryMarker) OR $ExpiryMarker > :now)"
        val attributes = Map(":now" -> AttributeValue.fromN(now.toString))
        (expression, attributes)
      } else {
        (s"attribute_not_exists($Deleted)", Map.empty[String, AttributeValue])
      }

    // FIXME is metadata needed here when includePayload==false? It is included in r2dbc
    val projectionExpression =
      if (includePayload) bySliceWithPayloadProjectionExpression else bySliceWithMetaProjectionExpression

    val req = QueryRequest.builder
      .tableName(settings.journalTable)
      .consistentRead(true)
      .keyConditionExpression(s"$Pid = :pid AND $SeqNr = :seqNr")
      .filterExpression(filterExpression)
      .expressionAttributeValues((attributeValues ++ filterAttributeValues).asJava)
      .projectionExpression(projectionExpression)
      .build()

    client
      .query(req)
      .asScala
      .flatMap { response =>
        val items = response.items()
        if (items.isEmpty)
          Future.successful(None)
        else {
          Future.successful(Some(createSerializedJournalItem(items.get(0), includePayload)))
        }
      }
      .recoverWith { case c: CompletionException =>
        Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
  }

  private def followBreadcrumb(evt: SerializedJournalItem): Future[SerializedJournalItem] =
    if (s3Fallback.nonEmpty && evt.serManifest == S3FallbackSerializer.BreadcrumbManifest) {
      deserializeBreadcrumb(evt).flatMap { bucket =>
        s3Fallback.get
          .loadEvent(evt.persistenceId, evt.seqNr, bucket, evt.payload.isDefined)
          .flatMap { maybeFromS3 =>
            // Unlike with a snapshot, we know that we're getting the event with a specific sequence number
            maybeFromS3 match {
              case Some(fromS3) =>
                Future.successful(
                  evt.copy(payload = fromS3.payload, serId = fromS3.serId, serManifest = fromS3.serManifest))

              case None =>
                val msg =
                  s"Failed to retrieve event from S3 for persistenceId=[${evt.persistenceId}], seqNr=[${evt.seqNr}]"
                logging.error(msg)
                Future.failed(new NoSuchElementException(msg))
            }
          }(ExecutionContext.parasitic)
      }
    } else Future.successful(evt)

  protected def deserializeBreadcrumb(breadcrumbItem: SerializedJournalItem): Future[String] =
    breadcrumbItem.payload match {
      case None =>
        // We don't want the ultimate event payload, but we need to deserialize the breadcrumb in order to fill in the manifest etc.
        queryForEvent(breadcrumbItem.persistenceId, breadcrumbItem.seqNr, true).flatMap { reloadedOpt =>
          reloadedOpt match {
            case None =>
              Future.failed(new IllegalStateException(
                s"Event for persistence ID [${breadcrumbItem.persistenceId}] at seqNr [${breadcrumbItem.seqNr}] disappeared"))
            case Some(reloaded) =>
              if (reloaded.serManifest != S3FallbackSerializer.BreadcrumbManifest)
                Future.failed(new IllegalStateException(
                  s"Event for persistence ID [${breadcrumbItem.persistenceId}] at seqNr [${breadcrumbItem.seqNr}] had unexpected manifest change"))
              else deserializeBreadcrumb(reloaded)
          }
        }(ExecutionContext.parasitic)

      case Some(payload) =>
        Future {
          val serialization = SerializationExtension(system)

          serialization.deserialize(payload, breadcrumbItem.serId, breadcrumbItem.serManifest).get match {
            case S3Breadcrumb(bucket) => bucket
            case o =>
              val msg =
                "Tried to decode apparent S3Breadcrumb (event) that wasn't " +
                s"(persistence ID [${breadcrumbItem.persistenceId}], seqNr [${breadcrumbItem.seqNr}])"
              logging.error(msg)
              throw new RuntimeException(msg)
          }
        }
    }
}
