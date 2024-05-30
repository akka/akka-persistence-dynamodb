/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

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

  def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalItem, NotUsed] = {

    if (toSequenceNr < fromSequenceNr) { // when max of 0
      Source.empty
    } else {
      val expressionAttributeValues =
        Map(
          ":pid" -> AttributeValue.fromS(persistenceId),
          ":from" -> AttributeValue.fromN(fromSequenceNr.toString),
          ":to" -> AttributeValue.fromN(toSequenceNr.toString)).asJava

      import JournalAttributes._
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
            tags = Set.empty, // FIXME
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

    import JournalAttributes._
    val req = QueryRequest.builder
      .tableName(settings.journalTable)
      .indexName(settings.journalBySliceGsi)
      .keyConditionExpression(s"$EntityTypeSlice = :entityTypeSlice AND $Timestamp BETWEEN :from AND :to")
      .filterExpression(s"attribute_not_exists($Deleted)")
      .expressionAttributeValues(expressionAttributeValues)
      // Limit won't limit the number of results you get with the paginator.
      // It only limits the number of results in each page
      // Limit is ignored by local DynamoDB.
      .limit(settings.querySettings.bufferSize)
      .build()

    // FIXME for backtracking we don't need all attributes, can be filtered with builder.attributesToGet

    val publisher = client.queryPaginator(req)

    Source.fromPublisher(publisher).mapConcat { response =>
      response.items().iterator().asScala.map { item =>
        val readTimestamp = InstantFactory.now()
        if (backtracking) {
          SerializedJournalItem(
            persistenceId = item.get(Pid).s(),
            seqNr = item.get(SeqNr).n().toLong,
            writeTimestamp = InstantFactory.fromEpochMicros(item.get(Timestamp).n().toLong),
            readTimestamp = readTimestamp,
            payload = None, // lazy loaded for backtracking
            serId = item.get(EventSerId).n().toInt,
            serManifest = "",
            writerUuid = "", // not need in this query
            tags = Set.empty, // FIXME
            metadata = None)
        } else {

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
            readTimestamp = readTimestamp,
            payload = Some(item.get(EventPayload).b().asByteArray()),
            serId = item.get(EventSerId).n().toInt,
            serManifest = item.get(EventSerManifest).s(),
            writerUuid = "", // not need in this query
            tags = Set.empty, // FIXME
            metadata = metadata)
        }
      }
    }
  }
}
