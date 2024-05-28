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
import software.amazon.awssdk.services.dynamodb.model.Delete
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.Update

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
      attributes.put(EntityType, AttributeValue.fromS(item.entityType))
      attributes.put(EventSerId, AttributeValue.fromN(item.serId.toString))
      attributes.put(EventSerManifest, AttributeValue.fromS(item.serManifest))
      attributes.put(EventPayload, AttributeValue.fromB(SdkBytes.fromByteArray(item.payload.get)))
      attributes.put(Writer, AttributeValue.fromS(item.writerUuid))

      item.metadata.foreach { meta =>
        attributes.put(MetaSerId, AttributeValue.fromN(meta.serId.toString))
        attributes.put(MetaSerManifest, AttributeValue.fromS(meta.serManifest))
        attributes.put(MetaPayload, AttributeValue.fromB(SdkBytes.fromByteArray(meta.payload)))
      }

      attributes
    }

    val totalEvents = events.size
    if (totalEvents == 1) {
      val req = PutItemRequest
        .builder()
        .tableName(settings.journalTable)
        .item(putItemAttributes(events.head))
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
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
            .put(Put.builder().tableName(settings.journalTable).item(putItemAttributes(item)).build())
            .build()
        }.asJava

      val req = TransactWriteItemsRequest
        .builder()
        .transactItems(writeItems)
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .build()

      val result = client.transactWriteItems(req).asScala

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

  def readHighestSequenceNr(persistenceId: String): Future[Long] = {
    import JournalAttributes._

    val attributeValues = Map(":pid" -> AttributeValue.fromS(persistenceId)).asJava

    val request = QueryRequest.builder
      .tableName(settings.journalTable)
      .consistentRead(true)
      .keyConditionExpression(s"$Pid = :pid")
      .expressionAttributeValues(attributeValues)
      .scanIndexForward(false) // get last item (highest sequence nr)
      .limit(1)
      .build()

    val result = client.query(request).asScala.map { response =>
      response.items().asScala.headOption.fold(0L) { item =>
        item.get(SeqNr).n().toLong
      }
    }

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Highest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  def readLowestSequenceNr(persistenceId: String): Future[Long] = {
    import JournalAttributes._

    val attributeValues = Map(":pid" -> AttributeValue.fromS(persistenceId)).asJava

    val request = QueryRequest.builder
      .tableName(settings.journalTable)
      .consistentRead(true)
      .keyConditionExpression(s"$Pid = :pid")
      .expressionAttributeValues(attributeValues)
      .scanIndexForward(true) // get first item (lowest sequence nr)
      .limit(1)
      .build()

    val result = client.query(request).asScala.map { response =>
      response.items().asScala.headOption.fold(0L) { item =>
        item.get(SeqNr).n().toLong
      }
    }

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Lowest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit] = {
    import JournalAttributes._

    def pk(pid: String, seqNr: Long): JHashMap[String, AttributeValue] = {
      val m = new JHashMap[String, AttributeValue]
      m.put(Pid, AttributeValue.fromS(pid))
      m.put(SeqNr, AttributeValue.fromN(seqNr.toString))
      m
    }

    def deleteBatch(from: Long, to: Long, lastBatch: Boolean): Future[Unit] = {
      val result = {
        val toSeqNr = if (lastBatch && !resetSequenceNumber) to - 1 else to
        val deleteItems =
          (from to toSeqNr).map { seqNr =>
            TransactWriteItem
              .builder()
              .delete(Delete.builder().tableName(settings.journalTable).key(pk(persistenceId, seqNr)).build())
              .build()
          }

        val writeItems =
          if (lastBatch && !resetSequenceNumber) {
            // update last item instead of deleting, keeping it as a tombstone to keep track of latest seqNr even
            // though all events have been deleted
            val deleteMarker =
              TransactWriteItem
                .builder()
                .update(Update
                  .builder()
                  .tableName(settings.journalTable)
                  .key(pk(persistenceId, to))
                  .updateExpression(
                    s"SET $Deleted = :del REMOVE $EventPayload, $EventSerId, $EventSerManifest, $Writer, $MetaPayload, $MetaSerId, $MetaSerManifest")
                  .expressionAttributeValues(Map(":del" -> AttributeValue.fromBool(true)).asJava)
                  .build())
                .build()
            deleteItems :+ deleteMarker
          } else
            deleteItems

        val req = TransactWriteItemsRequest
          .builder()
          .transactItems(writeItems.asJava)
          .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
          .build()

        client.transactWriteItems(req).asScala
      }

      if (log.isDebugEnabled()) {
        result.foreach { response =>
          log.debug(
            "Deleted events from [{}] to [{}] for persistenceId [{}], consumed [{}] WCU",
            from,
            to,
            persistenceId,
            response.consumedCapacity.iterator.asScala.map(_.capacityUnits.doubleValue()).sum)
        }
      }
      result.map(_ => ())(ExecutionContexts.parasitic)
    }

    // TransactWriteItems has a limit of 100
    val batchSize = 100

    def deleteInBatches(from: Long, maxTo: Long): Future[Unit] = {
      if (from + batchSize > maxTo) {
        deleteBatch(from, maxTo, lastBatch = true)
      } else {
        val to = from + batchSize - 1
        deleteBatch(from, to, lastBatch = false).flatMap(_ => deleteInBatches(to + 1, maxTo))
      }
    }

    val lowestSequenceNrForDelete = readLowestSequenceNr(persistenceId)
    val highestSeqNrForDelete =
      if (toSequenceNr == Long.MaxValue) readHighestSequenceNr(persistenceId)
      else Future.successful(toSequenceNr)

    for {
      fromSeqNr <- lowestSequenceNrForDelete
      toSeqNr <- highestSeqNrForDelete
      _ <- deleteInBatches(fromSeqNr, toSeqNr)
    } yield ()
  }

}
