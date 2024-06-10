/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant
import java.util.{ Map => JMap }

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.dynamodb.DynamoDBSettings
import akka.stream.scaladsl.Source
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

/**
 * INTERNAL API
 */
@InternalApi private[akka] class QueryDao(
    system: ActorSystem[_],
    settings: DynamoDBSettings,
    client: DynamoDbAsyncClient)
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

  def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalItem, NotUsed] = {
    import JournalAttributes._

    if (toSequenceNr < fromSequenceNr) { // when max of 0
      Source.empty
    } else {
      val expressionAttributeValues =
        Map(
          ":pid" -> AttributeValue.fromS(persistenceId),
          ":from" -> AttributeValue.fromN(fromSequenceNr.toString),
          ":to" -> AttributeValue.fromN(toSequenceNr.toString)).asJava

      val req = QueryRequest.builder
        .tableName(settings.journalTable)
        .consistentRead(true)
        .keyConditionExpression(s"$Pid = :pid AND $SeqNr BETWEEN :from AND :to")
        .filterExpression(s"attribute_not_exists($Deleted)")
        .expressionAttributeValues(expressionAttributeValues)
        .limit(settings.querySettings.bufferSize)
        .build()

      val publisher = client.queryPaginator(req)

      Source.fromPublisher(publisher).mapConcat { response =>
        response.items().iterator().asScala.map { item =>
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
  }

  // implements BySliceQuery.Dao
  override def itemsBySlice(
      entityType: String,
      slice: Int,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      backtracking: Boolean): Source[SerializedJournalItem, NotUsed] = {
    import JournalAttributes._

    val entityTypeSlice = s"$entityType-$slice"

    // FIXME we could look into using response.lastEvaluatedKey and use that as exclusiveStartKey in query,
    // instead of the timestamp for subsequent queries. Not sure how that works with GSI where the
    // sort key isn't unique (same timestamp). If DynamoDB can keep track of the exact offset and
    // not emit duplicates would not need the seen Map and that filter.
    // Well, we still need it for the first query because we want the external offset to be TimestampOffset
    // and that can include seen Map.

    val expressionAttributeValues =
      Map(
        ":entityTypeSlice" -> AttributeValue.fromS(entityTypeSlice),
        ":from" -> AttributeValue.fromN(InstantFactory.toEpochMicros(fromTimestamp).toString),
        ":to" -> AttributeValue.fromN(InstantFactory.toEpochMicros(toTimestamp).toString)).asJava

    val projectionExpression = if (backtracking) bySliceProjectionExpression else bySliceWithPayloadProjectionExpression

    val req = QueryRequest.builder
      .tableName(settings.journalTable)
      .indexName(settings.journalBySliceGsi)
      .keyConditionExpression(s"$EntityTypeSlice = :entityTypeSlice AND $Timestamp BETWEEN :from AND :to")
      .filterExpression(s"attribute_not_exists($Deleted)")
      .expressionAttributeValues(expressionAttributeValues)
      .projectionExpression(projectionExpression)
      // Limit won't limit the number of results you get with the paginator.
      // It only limits the number of results in each page
      // Limit is ignored by local DynamoDB.
      .limit(settings.querySettings.bufferSize)
      .build()

    val publisher = client.queryPaginator(req)

    Source.fromPublisher(publisher).mapConcat { response =>
      response.items().iterator().asScala.map { item =>
        if (backtracking) {
          SerializedJournalItem(
            persistenceId = item.get(Pid).s(),
            seqNr = item.get(SeqNr).n().toLong,
            writeTimestamp = InstantFactory.fromEpochMicros(item.get(Timestamp).n().toLong),
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
    val expressionAttributeValues =
      Map(":pid" -> AttributeValue.fromS(persistenceId), ":seqNr" -> AttributeValue.fromN(seqNr.toString)).asJava

    val req = QueryRequest.builder
      .tableName(settings.journalTable)
      .consistentRead(true)
      .keyConditionExpression(s"$Pid = :pid AND $SeqNr = :seqNr")
      .filterExpression(s"attribute_not_exists($Deleted)")
      .expressionAttributeValues(expressionAttributeValues)
      .projectionExpression(Timestamp)
      .build()

    client.query(req).asScala.map { response =>
      val items = response.items()
      if (items.isEmpty)
        None
      else {
        val timestampMicros = items.get(0).get(Timestamp).n().toLong
        Some(InstantFactory.fromEpochMicros(timestampMicros))
      }
    }
  }

  def loadEvent(persistenceId: String, seqNr: Long, includePayload: Boolean): Future[Option[SerializedJournalItem]] = {
    import JournalAttributes._
    val expressionAttributeValues =
      Map(":pid" -> AttributeValue.fromS(persistenceId), ":seqNr" -> AttributeValue.fromN(seqNr.toString)).asJava

    // FIXME is metadata needed here when includePayload==false? It is included in r2dbc
    val projectionExpression =
      if (includePayload) bySliceWithPayloadProjectionExpression else bySliceWithMetaProjectionExpression

    val req = QueryRequest.builder
      .tableName(settings.journalTable)
      .consistentRead(true)
      .keyConditionExpression(s"$Pid = :pid AND $SeqNr = :seqNr")
      .filterExpression(s"attribute_not_exists($Deleted)")
      .expressionAttributeValues(expressionAttributeValues)
      .projectionExpression(projectionExpression)
      .build()

    client.query(req).asScala.map { response =>
      val items = response.items()
      if (items.isEmpty)
        None
      else
        Some(createSerializedJournalItem(items.get(0), includePayload))
    }

  }

}
