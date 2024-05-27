/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.util.{ HashMap => JHashMap }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.dynamodb.DynamoDBSettings
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest

/**
 * INTERNAL API
 */
@InternalApi private[akka] object JournalDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[JournalDao])
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class JournalDao(
    system: ActorSystem[_],
    settings: DynamoDBSettings,
    client: DynamoDbAsyncClient) {
  import JournalDao._

  private val persistenceExt: Persistence = Persistence(system)

  private implicit val ec: ExecutionContext = system.executionContext

  def writeEvents(events: Seq[SerializedJournalItem]): Future[Done] = {
    require(events.nonEmpty)

    // it's always the same persistenceId for all events
    val persistenceId = events.head.persistenceId
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)

    def putItemAttributes(item: SerializedJournalItem) = {
      import JournalAttributes._
      val attributes = new JHashMap[String, AttributeValue]
      attributes.put(Pid, AttributeValue.fromS(persistenceId))
      attributes.put(SeqNr, AttributeValue.fromN(item.seqNr.toString))
      attributes.put(Slice, AttributeValue.fromN(slice.toString))
      attributes.put(EventSerId, AttributeValue.fromN(item.serId.toString))
      attributes.put(EventSerManifest, AttributeValue.fromS(item.serManifest))
      attributes.put(EventPayload, AttributeValue.fromB(SdkBytes.fromByteArray(item.payload.get)))
      attributes.put(Writer, AttributeValue.fromS(item.writerUuid))
      attributes
    }

    val totalEvents = events.size
    if (totalEvents == 1) {
      val req = PutItemRequest
        .builder()
        .item(putItemAttributes(events.head))
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .tableName(settings.journalTable)
        .build()
      val result = client.putItem(req).asScala

      if (log.isDebugEnabled()) {
        result.foreach { response =>
          log.debug(
            "Wrote [{}] events for persistenceId [{}], consumed [{}] WCU",
            1,
            persistenceId,
            response.consumedCapacity.capacityUnits)
        }
      }
      result.map(_ => Done)(ExecutionContexts.parasitic)
    } else {
      val writeItems =
        events.map { item =>
          TransactWriteItem
            .builder()
            .put(Put.builder().item(putItemAttributes(item)).tableName(settings.journalTable).build())
            .build()
        }.asJava

      val req = TransactWriteItemsRequest
        .builder()
        .transactItems(writeItems)
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .build()

      val result = client.transactWriteItems(req).asScala

      result.failed.foreach { exc => println(exc) }

      if (log.isDebugEnabled()) {
        result.foreach { response =>
          log.debug(
            "Wrote [{}] events for persistenceId [{}], consumed [{}] WCU",
            events.size,
            persistenceId,
            response.consumedCapacity.iterator.asScala.map(_.capacityUnits.doubleValue()).sum)
        }
      }
      result.map(_ => Done)(ExecutionContexts.parasitic)
    }

  }

}
