/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.control.NonFatal

import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.dynamodb.util.scaladsl.CreateTables
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.persistence.dynamodb"

  lazy val settings: DynamoDBSettings =
    DynamoDBSettings(typedSystem.settings.config.getConfig(testConfigPath))

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  lazy val client: DynamoDbAsyncClient = ClientProvider(typedSystem).clientFor(testConfigPath + ".client")

  lazy val localDynamoDB = ClientProvider(typedSystem).clientSettingsFor(testConfigPath + ".client").local.isDefined

  private lazy val log = LoggerFactory.getLogger(getClass)

  override protected def beforeAll(): Unit = {
    if (localDynamoDB) {
      try {
        Await.result(CreateTables.createJournalTable(typedSystem, settings, client, deleteIfExists = true), 10.seconds)
        Await.result(
          CreateTables.createSnapshotsTable(typedSystem, settings, client, deleteIfExists = true),
          10.seconds)
      } catch {
        case NonFatal(ex) => throw new RuntimeException(s"Test db creation failed", ex)
      }
    }

    super.beforeAll()
  }

  // to be able to store events with specific timestamps
  def writeEvent(slice: Int, persistenceId: PersistenceId, seqNr: Long, timestamp: Instant, event: String): Unit = {
    import java.util.{ HashMap => JHashMap }

    import akka.persistence.dynamodb.internal.JournalAttributes._

    log.debug("Write test event [{}] [{}] [{}] at time [{}]", persistenceId, seqNr, event, timestamp)

    val stringSerializer = SerializationExtension(typedSystem).serializerFor(classOf[String])

    val attributes = new JHashMap[String, AttributeValue]
    attributes.put(Pid, AttributeValue.fromS(persistenceId.id))
    attributes.put(SeqNr, AttributeValue.fromN(seqNr.toString))
    attributes.put(EntityTypeSlice, AttributeValue.fromS(s"${persistenceId.entityTypeHint}-$slice"))
    val timestampMicros = InstantFactory.toEpochMicros(timestamp)
    attributes.put(Timestamp, AttributeValue.fromN(timestampMicros.toString))
    attributes.put(EventSerId, AttributeValue.fromN(stringSerializer.identifier.toString))
    attributes.put(EventSerManifest, AttributeValue.fromS(""))
    attributes.put(EventPayload, AttributeValue.fromB(SdkBytes.fromByteArray(stringSerializer.toBinary(event))))
    attributes.put(Writer, AttributeValue.fromS(""))

    val req = PutItemRequest
      .builder()
      .tableName(settings.journalTable)
      .item(attributes)
      .build()
    Await.result(client.putItem(req).asScala, 10.seconds)
  }

  // directly get event items from the journal table
  def getEventItemsFor(persistenceId: String): Seq[Map[String, AttributeValue]] = {
    import akka.persistence.dynamodb.internal.JournalAttributes.Pid
    val request = QueryRequest.builder
      .tableName(settings.journalTable)
      .consistentRead(true)
      .keyConditionExpression(s"$Pid = :pid")
      .expressionAttributeValues(Map(":pid" -> AttributeValue.fromS(persistenceId)).asJava)
      .build()
    Await.result(client.query(request).asScala, 10.seconds).items.asScala.toSeq.map(_.asScala.toMap)
  }

  // directly get a snapshot item from the snapshot table
  def getSnapshotItemFor(persistenceId: String): Option[Map[String, AttributeValue]] = {
    import akka.persistence.dynamodb.internal.SnapshotAttributes.Pid
    val request = QueryRequest.builder
      .tableName(settings.snapshotTable)
      .consistentRead(true)
      .keyConditionExpression(s"$Pid = :pid")
      .expressionAttributeValues(Map(":pid" -> AttributeValue.fromS(persistenceId)).asJava)
      .build()
    Await.result(client.query(request).asScala, 10.seconds).items.asScala.headOption.map(_.asScala.toMap)
  }

}
