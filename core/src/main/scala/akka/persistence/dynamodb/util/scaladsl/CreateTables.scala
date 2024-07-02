/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.util.scaladsl

import java.util.concurrent.CompletionException

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.FutureConverters._
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.dynamodb.DynamoDBSettings
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.Projection
import software.amazon.awssdk.services.dynamodb.model.ProjectionType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

object CreateTables {

  def createJournalTable(
      system: ActorSystem[_],
      settings: DynamoDBSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean): Future[Done] = {
    import akka.persistence.dynamodb.internal.JournalAttributes._
    implicit val ec: ExecutionContext = system.executionContext

    val existingTable =
      client.describeTable(DescribeTableRequest.builder().tableName(settings.journalTable).build()).asScala

    def create(): Future[Done] = {
      val sliceIndex = GlobalSecondaryIndex
        .builder()
        .indexName(settings.journalBySliceGsi)
        .keySchema(
          KeySchemaElement.builder().attributeName(EntityTypeSlice).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(Timestamp).keyType(KeyType.RANGE).build())
        .projection(
          // FIXME we could skip a few attributes
          Projection.builder().projectionType(ProjectionType.ALL).build())
        // FIXME config
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
        .build()

      val req = CreateTableRequest
        .builder()
        .tableName(settings.journalTable)
        .keySchema(
          KeySchemaElement.builder().attributeName(Pid).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(SeqNr).keyType(KeyType.RANGE).build())
        .attributeDefinitions(
          AttributeDefinition.builder().attributeName(Pid).attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition.builder().attributeName(SeqNr).attributeType(ScalarAttributeType.N).build(),
          AttributeDefinition.builder().attributeName(EntityTypeSlice).attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition.builder().attributeName(Timestamp).attributeType(ScalarAttributeType.N).build())
        // FIXME config
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
        .globalSecondaryIndexes(sliceIndex)
        .build()

      client.createTable(req).asScala.map(_ => Done)
    }

    def delete(): Future[Done] = {
      val req = DeleteTableRequest.builder().tableName(settings.journalTable).build()
      client.deleteTable(req).asScala.map(_ => Done)
    }

    existingTable.transformWith {
      case Success(_) =>
        if (deleteIfExists) delete().flatMap(_ => create())
        else Future.successful(Done)
      case Failure(_: ResourceNotFoundException) => create()
      case Failure(exception: CompletionException) =>
        exception.getCause match {
          case _: ResourceNotFoundException => create()
          case cause                        => Future.failed[Done](cause)
        }
      case Failure(exc) =>
        Future.failed[Done](exc)
    }
  }

  def createSnapshotsTable(
      system: ActorSystem[_],
      settings: DynamoDBSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean): Future[Done] = {
    import akka.persistence.dynamodb.internal.SnapshotAttributes._

    implicit val ec: ExecutionContext = system.executionContext

    val existingTable =
      client.describeTable(DescribeTableRequest.builder().tableName(settings.snapshotTable).build()).asScala

    def create(): Future[Done] = {
      val request = CreateTableRequest
        .builder()
        .tableName(settings.snapshotTable)
        .keySchema(KeySchemaElement.builder().attributeName(Pid).keyType(KeyType.HASH).build())
        .attributeDefinitions(
          AttributeDefinition.builder().attributeName(Pid).attributeType(ScalarAttributeType.S).build())
        // FIXME config
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
        .build()

      client
        .createTable(request)
        .asScala
        .map(_ => Done)
        .recoverWith { case c: CompletionException =>
          Future.failed(c.getCause)
        }(ExecutionContexts.parasitic)
    }

    def delete(): Future[Done] = {
      val req = DeleteTableRequest.builder().tableName(settings.snapshotTable).build()
      client
        .deleteTable(req)
        .asScala
        .map(_ => Done)
        .recoverWith { case c: CompletionException =>
          Future.failed(c.getCause)
        }(ExecutionContexts.parasitic)
    }

    existingTable.transformWith {
      case Success(_) =>
        if (deleteIfExists) delete().flatMap(_ => create())
        else Future.successful(Done)
      case Failure(_: ResourceNotFoundException) => create()
      case Failure(exception: CompletionException) =>
        exception.getCause match {
          case _: ResourceNotFoundException => create()
          case cause                        => Future.failed[Done](cause)
        }
      case Failure(exception) => Future.failed[Done](exception)
    }
  }

}
