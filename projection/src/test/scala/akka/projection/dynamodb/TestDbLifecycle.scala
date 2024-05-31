/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.time.Instant
import java.util.concurrent.CompletionException

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.FutureConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.dynamodb.ClientProvider
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.dynamodb.internal.JournalAttributes
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.Projection
import software.amazon.awssdk.services.dynamodb.model.ProjectionType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.projection.dynamodb"

  lazy val settings: DynamoDBProjectionSettings =
    DynamoDBProjectionSettings(typedSystem.settings.config.getConfig(testConfigPath))

  lazy val dynamoDBSettings: DynamoDBSettings =
    DynamoDBSettings(
      typedSystem.settings.config
        .getConfig(settings.useClient.replace(".client", "")))

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  lazy val client: DynamoDbAsyncClient = ClientProvider(typedSystem).clientFor(settings.useClient)

  private lazy val log = LoggerFactory.getLogger(getClass)

  override protected def beforeAll(): Unit = {
    try {
      Await.result(createJournalTable(), 10.seconds)
      Await.result(createTimestampOffsetStoreTable(), 10.seconds)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Test db creation failed", ex)
    }

    super.beforeAll()
  }

  private def createJournalTable(): Future[Done] = {
    import akka.persistence.dynamodb.internal.JournalAttributes._
    implicit val ec: ExecutionContext = typedSystem.executionContext

    val existingTable =
      client.describeTable(DescribeTableRequest.builder().tableName(dynamoDBSettings.journalTable).build()).asScala

    def create(): Future[Done] = {
      val sliceIndex = GlobalSecondaryIndex
        .builder()
        .indexName(dynamoDBSettings.journalBySliceGsi)
        .keySchema(
          KeySchemaElement.builder().attributeName(EntityTypeSlice).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(Timestamp).keyType(KeyType.RANGE).build())
        .projection(
          Projection.builder().projectionType(ProjectionType.ALL).build()
        ) // FIXME we could skip a few attributes
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
        .build()

      val req = CreateTableRequest
        .builder()
        .tableName(dynamoDBSettings.journalTable)
        .keySchema(
          KeySchemaElement.builder().attributeName(Pid).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(SeqNr).keyType(KeyType.RANGE).build())
        .attributeDefinitions(
          AttributeDefinition.builder().attributeName(Pid).attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition.builder().attributeName(SeqNr).attributeType(ScalarAttributeType.N).build(),
          AttributeDefinition.builder().attributeName(EntityTypeSlice).attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition.builder().attributeName(Timestamp).attributeType(ScalarAttributeType.N).build())
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
        .globalSecondaryIndexes(sliceIndex)
        .build()

      client.createTable(req).asScala.map(_ => Done)(typedSystem.executionContext)
    }

    def delete(): Future[Done] = {
      val req = DeleteTableRequest.builder().tableName(dynamoDBSettings.journalTable).build()
      client.deleteTable(req).asScala.map(_ => Done)(typedSystem.executionContext)
    }

    existingTable.transformWith {
      case Success(_)                            => delete().flatMap(_ => create())
      case Failure(_: ResourceNotFoundException) => create()
      case Failure(exception: CompletionException) =>
        exception.getCause match {
          case _: ResourceNotFoundException => create()
          case failure                      => Future.failed[Done](failure)
        }
      case Failure(exc) =>
        Future.failed[Done](exc)
    }
  }

  private def createTimestampOffsetStoreTable(): Future[Done] = {
    import akka.projection.dynamodb.internal.OffsetStoreDao.OffsetStoreAttributes._
    implicit val ec: ExecutionContext = typedSystem.executionContext

    val existingTable =
      client.describeTable(DescribeTableRequest.builder().tableName(settings.timestampOffsetTable).build()).asScala

    def create(): Future[Done] = {
      val req = CreateTableRequest
        .builder()
        .tableName(settings.timestampOffsetTable)
        .keySchema(
          KeySchemaElement.builder().attributeName(NameSlice).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(Pid).keyType(KeyType.RANGE).build())
        .attributeDefinitions(
          AttributeDefinition.builder().attributeName(NameSlice).attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition.builder().attributeName(Pid).attributeType(ScalarAttributeType.S).build())
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
        .build()

      client.createTable(req).asScala.map(_ => Done)(typedSystem.executionContext)
    }

    def delete(): Future[Done] = {
      val req = DeleteTableRequest.builder().tableName(settings.timestampOffsetTable).build()
      client.deleteTable(req).asScala.map(_ => Done)(typedSystem.executionContext)
    }

    existingTable.transformWith {
      case Success(_)                            => delete().flatMap(_ => create())
      case Failure(_: ResourceNotFoundException) => create()
      case Failure(exception: CompletionException) =>
        exception.getCause match {
          case _: ResourceNotFoundException => create()
          case failure                      => Future.failed[Done](failure)
        }
      case Failure(exc) =>
        Future.failed[Done](exc)
    }
  }

}
