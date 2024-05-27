/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant

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
    client: DynamoDbAsyncClient) {
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
        .keyConditionExpression(s"$Pid = :pid AND $SeqNr BETWEEN :from AND :to")
        .expressionAttributeValues(expressionAttributeValues)
        .build()

      val publisher = client.queryPaginator(req)

      Source.fromPublisher(publisher).mapConcat { response =>
        response.items().iterator().asScala.map { item =>
          // FIXME read all attributes
          SerializedJournalItem(
            slice = item.get(Slice).n().toInt,
            entityType = "",
            persistenceId = item.get(Pid).s(),
            seqNr = item.get(SeqNr).n().toLong,
            writeTimestamp = Instant.EPOCH,
            payload = Some(item.get(EventPayload).b().asByteArray()),
            serId = item.get(EventSerId).n().toInt,
            serManifest = item.get(EventSerManifest).s(),
            writerUuid = item.get(Writer).s(),
            tags = Set.empty,
            metadata = None)
        }
      }
    }
  }

}
