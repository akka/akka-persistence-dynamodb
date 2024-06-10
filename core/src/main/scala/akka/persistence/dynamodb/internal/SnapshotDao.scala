/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant
import java.util.concurrent.CompletionException
import java.util.{ HashMap => JHashMap }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.typed.PersistenceId
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
    client: DynamoDbAsyncClient) {
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

    client.query(request).asScala.map { response =>
      val items = response.items()
      if (items.isEmpty) {
        None
      } else {
        val item = response.items.get(0)

        val metadata = Option(item.get(MetaPayload)).map { metaPayload =>
          SerializedSnapshotMetadata(
            serId = item.get(MetaSerId).n().toInt,
            serManifest = item.get(MetaSerManifest).s(),
            payload = metaPayload.b().asByteArray())
        }

        val snapshot = SerializedSnapshotItem(
          persistenceId = item.get(Pid).s(),
          seqNr = item.get(SeqNr).n().toLong,
          writeTimestamp = Instant.ofEpochMilli(item.get(WriteTimestamp).n().toLong),
          eventTimestamp = InstantFactory.fromEpochMicros(item.get(EventTimestamp).n().toLong),
          payload = item.get(SnapshotPayload).b().asByteArray(),
          serId = item.get(SnapshotSerId).n().toInt,
          serManifest = item.get(SnapshotSerManifest).s(),
          tags = if (item.containsKey(Tags)) item.get(Tags).ss().asScala.toSet else Set.empty,
          metadata = metadata)

        log.debug(
          "Loaded snapshot for persistenceId [{}], consumed [{}] RCU",
          persistenceId,
          response.consumedCapacity.capacityUnits)

        Some(snapshot)
      }
    }
  }

  def store(snapshot: SerializedSnapshotItem): Future[Unit] = {
    import SnapshotAttributes._

    // TODO: check total size of snapshot (limit of 400 kB), store over multiple parts?

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

    result.map(_ => ())(ExecutionContexts.parasitic)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    import SnapshotAttributes._

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
      .map(_ => ())(ExecutionContexts.parasitic)
      // ignore if the criteria conditional check failed
      .recover {
        case _: ConditionalCheckFailedException => ()
        case e: CompletionException =>
          e.getCause match {
            case _: ConditionalCheckFailedException => ()
            case failure                            => throw failure
          }
      }(ExecutionContexts.parasitic)
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
