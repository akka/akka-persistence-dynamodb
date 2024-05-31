/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.internal

import java.util.{ HashMap => JHashMap }
import java.time.Instant
import java.util.Collections

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.dynamodb.internal.InstantFactory
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.internal.DynamoDBOffsetStore.Record
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.WriteRequest

/**
 * INTERNAL API
 */
@InternalApi private[projection] object OffsetStoreDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[OffsetStoreDao])

  object OffsetStoreAttributes {
    // FIXME should attribute names be shorter?
    val Pid = "pid"
    val SeqNr = "seq_nr"
    val NameSlice = "name_slice"
    val Timestamp = "ts"

    // FIXME empty string not allowed
    val timestampBySlicePid = AttributeValue.fromS("_")
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[projection] class OffsetStoreDao(
    system: ActorSystem[_],
    settings: DynamoDBProjectionSettings,
    projectionId: ProjectionId,
    client: DynamoDbAsyncClient) {
  import OffsetStoreDao.log
  import system.executionContext

  private def nameSlice(slice: Int): String = s"${projectionId.name}-$slice"

  def latestTimestamp(slice: Int): Future[Option[Instant]] = {
    import OffsetStoreDao.OffsetStoreAttributes._
    val expressionAttributeValues =
      Map(":nameSlice" -> AttributeValue.fromS(nameSlice(slice)), ":pid" -> timestampBySlicePid).asJava

    val req = QueryRequest.builder
      .tableName(settings.timestampOffsetTable)
      .consistentRead(false) // not necessary to read latest, can start at earlier time
      .keyConditionExpression(s"$NameSlice = :nameSlice AND $Pid = :pid")
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

  def storeLatestTimestamps(timestampsBySlice: Map[Int, Instant]): Future[Done] = {
    import OffsetStoreDao.OffsetStoreAttributes._

    // FIXME upper limit on number of writes in a batch, split up

    val writeItems =
      timestampsBySlice
        .map { case (slice, timestamp) =>
          val attributes = new JHashMap[String, AttributeValue]
          attributes.put(NameSlice, AttributeValue.fromS(nameSlice(slice)))
          attributes.put(Pid, timestampBySlicePid)
          val timestampMicros = InstantFactory.toEpochMicros(timestamp)
          attributes.put(Timestamp, AttributeValue.fromN(timestampMicros.toString))

          WriteRequest.builder
            .putRequest(
              PutRequest
                .builder()
                .item(attributes)
                .build())
            .build()
        }
        .toVector
        .asJava

    val req = BatchWriteItemRequest
      .builder()
      .requestItems(Collections.singletonMap(settings.timestampOffsetTable, writeItems))
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()

    val result = client.batchWriteItem(req).asScala

    if (log.isDebugEnabled()) {
      result.foreach { response =>
        log.debug(
          "Wrote latest timestamps for [{}] slices, consumed [{}] WCU",
          timestampsBySlice.size,
          response.consumedCapacity.iterator.asScala.map(_.capacityUnits.doubleValue()).sum)
      }
    }

    result.map(_ => Done)(ExecutionContexts.parasitic)
  }

  def storeSequenceNumbers(records: IndexedSeq[Record]): Future[Done] = {
    import OffsetStoreDao.OffsetStoreAttributes._

    // FIXME upper limit on number of writes in a batch, split up

    val writeItems =
      records
        .map { case Record(slice, pid, seqNr, timestamp) =>
          val attributes = new JHashMap[String, AttributeValue]
          attributes.put(NameSlice, AttributeValue.fromS(nameSlice(slice)))
          attributes.put(Pid, AttributeValue.fromS(pid))
          attributes.put(SeqNr, AttributeValue.fromN(seqNr.toString))
          val timestampMicros = InstantFactory.toEpochMicros(timestamp)
          attributes.put(Timestamp, AttributeValue.fromN(timestampMicros.toString))

          WriteRequest.builder
            .putRequest(
              PutRequest
                .builder()
                .item(attributes)
                .build())
            .build()
        }
        .toVector
        .asJava

    val req = BatchWriteItemRequest
      .builder()
      .requestItems(Collections.singletonMap(settings.timestampOffsetTable, writeItems))
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .build()

    val result = client.batchWriteItem(req).asScala

    if (log.isDebugEnabled()) {
      result.foreach { response =>
        log.debug(
          "Wrote [{}] sequence numbers, consumed [{}] WCU",
          records.size,
          response.consumedCapacity.iterator.asScala.map(_.capacityUnits.doubleValue()).sum)
      }
    }

    result.map(_ => Done)(ExecutionContexts.parasitic)
  }

  def load(slice: Int, pid: String): Future[Option[Record]] = {
    import OffsetStoreDao.OffsetStoreAttributes._
    val expressionAttributeValues =
      Map(":nameSlice" -> AttributeValue.fromS(nameSlice(slice)), ":pid" -> AttributeValue.fromS(pid)).asJava

    val req = QueryRequest.builder
      .tableName(settings.timestampOffsetTable)
      .consistentRead(true)
      .keyConditionExpression(s"$NameSlice = :nameSlice AND $Pid = :pid")
      .expressionAttributeValues(expressionAttributeValues)
      .projectionExpression(s"$SeqNr, $Timestamp")
      .build()

    client.query(req).asScala.map { response =>
      val items = response.items()
      if (items.isEmpty)
        None
      else {
        val item = items.get(0)
        val seqNr = item.get(SeqNr).n().toLong
        val timestampMicros = item.get(Timestamp).n().toLong
        val timestamp = InstantFactory.fromEpochMicros(timestampMicros)
        Some(DynamoDBOffsetStore.Record(slice, pid, seqNr, timestamp))
      }
    }
  }
}
