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
import scala.util.Failure
import scala.util.Success

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import scala.concurrent.Promise

/**
 * INTERNAL API
 */
@InternalApi private[akka] class QueryDao(
    system: ActorSystem[_],
    settings: DynamoDBSettings,
    client: DynamoDbAsyncClient)
    extends BySliceQuery.Dao[SerializedJournalItem] {
  import system.executionContext

  private val serialization = SerializationExtension(system)
  private val fallbackStoreProvider = FallbackStoreProvider(system)

  private val _backtrackingBreadcrumbSerId = Promise[Int]()
  def backtrackingBreadcrumbSerId(): Int = {
    _backtrackingBreadcrumbSerId.future.value match {
      case Some(v) => v.get
      case None =>
        val serializerIds = serialization.bindings.iterator.map(_._2.identifier).toSet
        // Use an unfolding iterator to find a serializer ID that's not bound.  There shouldn't ever be more
        // than a few thousand serializer IDs bound (and the docs suggest real serializers should only have
        // positive serializer IDs ("couple of billion")), so this shouldn't have to iterate far.
        //
        // Ranges that aren't indexable by an Int (viz. have more than Int.MaxValue elements) are surprisingly
        // restrictive, thus the unfold
        Iterator
          .unfold(Int.MinValue) { s =>
            if (s != Int.MaxValue) Some(s -> (s + 1))
            else None
          }
          // stop fast if some other thread found this before we do
          .find { i => !serializerIds(i) || _backtrackingBreadcrumbSerId.isCompleted } match {
          case None =>
            // Over 4 billion serializers... really?
            _backtrackingBreadcrumbSerId.tryFailure(new NoSuchElementException("All serializer IDs used?"))

          case Some(id) =>
            _backtrackingBreadcrumbSerId.trySuccess(id)
        }
        // first get is safe, we just ensured completion
        // second get will throw if we exhausted serializers, but that's a danger we're prepared to face...
        _backtrackingBreadcrumbSerId.future.value.get.get
    }
  }

  private val bySliceProjectionExpression = {
    import JournalAttributes._
    s"$Pid, $SeqNr, $Timestamp, $EventSerId, $EventSerManifest, $Tags, $BreadcrumbSerId, $BreadcrumbSerManifest"
  }

  private val bySliceWithMetaProjectionExpression = {
    import JournalAttributes._
    s"$bySliceProjectionExpression, $MetaSerId, $MetaSerManifest, $MetaPayload"
  }

  private val bySliceWithPayloadProjectionExpression = {
    import JournalAttributes._
    s"$bySliceWithMetaProjectionExpression, $EventPayload, $BreadcrumbPayload"
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

              val seqNr = item.get(SeqNr).n().toLong
              val writeTimestamp = InstantFactory.fromEpochMicros(item.get(Timestamp).n().toLong)
              val writerUuid = item.get(Writer).s()
              val tags: Set[String] = if (item.containsKey(Tags)) item.get(Tags).ss().asScala.toSet else Set.empty

              // events are saved with regular payload or a breadcrumb, but not both
              if (item.containsKey(EventPayload)) {
                SerializedJournalItem(
                  persistenceId = persistenceId,
                  seqNr = seqNr,
                  writeTimestamp = writeTimestamp,
                  readTimestamp = InstantFactory.EmptyTimestamp,
                  payload = Some(item.get(EventPayload).b().asByteArray()),
                  serId = item.get(EventSerId).n().toInt,
                  serManifest = item.get(EventSerManifest).s(),
                  writerUuid = item.get(Writer).s(),
                  tags = tags,
                  metadata = metadata)
              } else if (item.containsKey(BreadcrumbPayload)) {
                JournalItemWithBreadcrumb(
                  persistenceId = persistenceId,
                  seqNr = seqNr,
                  writeTimestamp = writeTimestamp,
                  readTimestamp = InstantFactory.EmptyTimestamp,
                  writerUuid = writerUuid,
                  tags = tags,
                  metadata = metadata,
                  breadcrumbSerId = item.get(BreadcrumbSerId).n().toInt,
                  breadcrumbSerManifest = item.get(BreadcrumbSerManifest).s(),
                  breadcrumbPayload = Some(item.get(BreadcrumbPayload).b().asByteArray()))
              } else {
                // neither?
                val msg =
                  s"Event in journal for persistenceId [$persistenceId] seqNr [$seqNr] is missing both event payload and breadcrumb"
                logging.error(msg)
                throw new IllegalStateException(msg)
              }
            }
          }
        }
        .mapAsync(settings.journalFallbackSettings.batchSize)(followBreadcrumb)
        .mapError { case c: CompletionException =>
          c.getCause
        }
    }
  }

  // implements BySliceQuery.Dao
  // NB: if backtracking and an event that was saved to the fallback store is encountered,
  // the payload will be None (as with any backtracking event) and the serId of the returned
  // item will be one not used by any bound serializer.
  //
  // Without a payload, the serializer ID is kind of meaningless (and the events-by-slice
  // queries in the read journal will ignore the serializer ID unless it is the filtered
  // payload serializer).
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
              serId =
                if (item.containsKey(EventSerId)) item.get(EventSerId).n().toInt else backtrackingBreadcrumbSerId(),
              serManifest = "",
              writerUuid = "", // not need in this query
              tags = if (item.containsKey(Tags)) item.get(Tags).ss().asScala.toSet else Set.empty,
              metadata = None)
          } else {
            createItemFromJournal(item, includePayload = true)
          }
        }
        .mapAsync(settings.journalFallbackSettings.batchSize)(followBreadcrumb)
        .mapError { case c: CompletionException =>
          c.getCause
        }
    }
  }

  private def createItemFromJournal(item: JMap[String, AttributeValue], includePayload: Boolean): ItemInJournal = {
    import JournalAttributes._

    val metadata = Option(item.get(MetaPayload)).map { metaPayload =>
      SerializedEventMetadata(
        serId = item.get(MetaSerId).n().toInt,
        serManifest = item.get(MetaSerManifest).s(),
        payload = metaPayload.b().asByteArray())
    }

    val pid = item.get(Pid).s
    val seqNr = item.get(SeqNr).n.toLong
    val writeTimestamp = InstantFactory.fromEpochMicros(item.get(Timestamp).n.toLong)
    val tags: Set[String] = if (item.containsKey(Tags)) item.get(Tags).ss.asScala.toSet else Set.empty

    if (item.containsKey(EventSerId)) {
      SerializedJournalItem(
        persistenceId = pid,
        seqNr = seqNr,
        writeTimestamp = writeTimestamp,
        readTimestamp = InstantFactory.now(),
        payload = if (includePayload) Some(item.get(EventPayload).b().asByteArray()) else None,
        serId = item.get(EventSerId).n().toInt,
        serManifest = item.get(EventSerManifest).s(),
        writerUuid = "", // not need in this query
        tags = tags,
        metadata = metadata)
    } else if (item.containsKey(BreadcrumbSerId)) {
      JournalItemWithBreadcrumb(
        persistenceId = pid,
        seqNr = seqNr,
        writeTimestamp = writeTimestamp,
        readTimestamp = InstantFactory.now(),
        writerUuid = "", // Not needed in this query
        tags = tags,
        metadata = metadata,
        breadcrumbSerId = item.get(BreadcrumbSerId).n.toInt,
        breadcrumbSerManifest = item.get(BreadcrumbSerManifest).s,
        breadcrumbPayload = if (includePayload) Some(item.get(BreadcrumbPayload).b.asByteArray) else None)
    } else {
      throw new IllegalStateException("Encountered event in journal which had neither payload nor breadcrumb")
    }
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
        case None =>
          // safe cast as this is equivalent to Future.successful(None)
          queryResult.asInstanceOf[Future[Option[SerializedJournalItem]]]
        case Some(item) => followBreadcrumb(item).map(Some.apply)(ExecutionContext.parasitic)
      }
    }
  }

  private def queryForEvent(
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[ItemInJournal]] = {
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
          Future.successful(Some(createItemFromJournal(items.get(0), includePayload)))
        }
      }
      .recoverWith { case c: CompletionException =>
        Future.failed(c.getCause)
      }(ExecutionContext.parasitic)
  }

  private def followBreadcrumb(item: ItemInJournal): Future[SerializedJournalItem] =
    item match {
      case sji: SerializedJournalItem => Future.successful(sji)

      case jiwb: JournalItemWithBreadcrumb =>
        deserializeBreadcrumb(jiwb).flatMap { crumb =>
          val fallbackStore = fallbackStoreProvider.eventFallbackStoreFor(settings.journalFallbackSettings.plugin)

          val includePayload = jiwb.breadcrumbPayload.isDefined

          fallbackStore
            .loadEvent(crumb, jiwb.persistenceId, jiwb.seqNr, includePayload)
            .flatMap { maybeFromFallback =>
              maybeFromFallback match {
                case Some(fromFallback) =>
                  Future.successful(SerializedJournalItem(
                    persistenceId = jiwb.persistenceId,
                    seqNr = jiwb.seqNr,
                    writeTimestamp = jiwb.writeTimestamp,
                    readTimestamp = InstantFactory.now(),
                    payload = fromFallback.payload,
                    serId = fromFallback.serId,
                    serManifest = fromFallback.serManifest,
                    writerUuid = jiwb.writerUuid,
                    tags = jiwb.tags,
                    metadata = jiwb.metadata))
                case None =>
                  val msg =
                    s"Failed to retrieve event from fallback store for persistenceId [${jiwb.persistenceId}] seqNr [${jiwb.seqNr}]"
                  logging.error(msg)
                  Future.failed(new NoSuchElementException(msg))
              }
            }(ExecutionContext.parasitic)
        }
    }

  private def deserializeBreadcrumb(item: JournalItemWithBreadcrumb): Future[AnyRef] =
    if (settings.journalFallbackSettings.isEnabled) {
      val fallbackStore = fallbackStoreProvider.eventFallbackStoreFor(settings.journalFallbackSettings.plugin)

      item.breadcrumbPayload match {
        case Some(payload) =>
          serialization.deserialize(payload, item.breadcrumbSerId, item.breadcrumbSerManifest) match {
            case Success(candidate) =>
              fallbackStore.toBreadcrumb(candidate) match {
                case Some(breadcrumb) => Future.successful(breadcrumb)
                case None =>
                  val msg =
                    s"Breadcrumb rejected by fallback store at persistenceId [${item.persistenceId}] seqNr [${item.seqNr}]"
                  logging.error(msg)
                  Future.failed(new IllegalStateException(msg))
              }

            case Failure(ex) =>
              val msg =
                s"Failed to deserialize breadcrumb at persistenceId [${item.persistenceId}] seqNr [${item.seqNr}]"
              logging.error(ex, msg)
              Future.failed(ex)
          }

        case None =>
          // We don't want the ultimate event payload, but we need to deserialize the breadcrumb in order to fill in the manifest etc.
          queryForEvent(item.persistenceId, item.seqNr, true).flatMap { reloadedOpt =>
            reloadedOpt match {
              case Some(reloaded: JournalItemWithBreadcrumb) =>
                deserializeBreadcrumb(reloaded)

              case None =>
                Future.failed(
                  new IllegalStateException(
                    s"Event for persistenceId [${item.persistenceId}] at seqNr [${item.seqNr}] disappeared"))

              case Some(reloaded: SerializedJournalItem) =>
                Future.failed(new IllegalStateException(
                  s"Event for persistenceId [${item.persistenceId}] at seqNr [${item.seqNr}] changed from breadcrumb to payload"))
            }
          }
      }
    } else {
      val log =
        s"Journal fallback not enabled but encountered event with breadcrumb (persistenceId ${item.persistenceId} seqNr ${item.seqNr}"
      logging.error(log)
      Future.failed(new IllegalStateException(log))
    }
}
