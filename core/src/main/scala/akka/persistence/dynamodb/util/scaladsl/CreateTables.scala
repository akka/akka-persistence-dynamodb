/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
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
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.util.IndexSettings
import akka.persistence.dynamodb.util.OnDemandThroughputSettings
import akka.persistence.dynamodb.util.ProvisionedThroughputSettings
import akka.persistence.dynamodb.util.TableSettings
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.OnDemandThroughput
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
      deleteIfExists: Boolean,
      tableSettings: TableSettings = TableSettings.Local,
      sliceIndexSettings: IndexSettings = IndexSettings.Local): Future[Done] = {
    import akka.persistence.dynamodb.internal.JournalAttributes._
    implicit val ec: ExecutionContext = system.executionContext

    val existingTable =
      client.describeTable(DescribeTableRequest.builder().tableName(settings.journalTable).build()).asScala

    def create(): Future[Done] = {
      val sliceIndex = if (sliceIndexSettings.enabled) {
        var indexBuilder =
          GlobalSecondaryIndex.builder
            .indexName(settings.journalBySliceGsi)
            .keySchema(
              KeySchemaElement.builder().attributeName(EntityTypeSlice).keyType(KeyType.HASH).build(),
              KeySchemaElement.builder().attributeName(Timestamp).keyType(KeyType.RANGE).build())
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())

        indexBuilder = sliceIndexSettings.throughput match {
          case provisioned: ProvisionedThroughputSettings =>
            indexBuilder.provisionedThroughput(
              ProvisionedThroughput.builder
                .readCapacityUnits(provisioned.readCapacityUnits)
                .writeCapacityUnits(provisioned.writeCapacityUnits)
                .build())
          case onDemand: OnDemandThroughputSettings =>
            indexBuilder.onDemandThroughput(
              OnDemandThroughput.builder
                .maxReadRequestUnits(onDemand.maxReadRequestUnits)
                .maxWriteRequestUnits(onDemand.maxWriteRequestUnits)
                .build())
        }

        Some(indexBuilder.build())
      } else None

      var requestBuilder = CreateTableRequest.builder
        .tableName(settings.journalTable)
        .keySchema(
          KeySchemaElement.builder().attributeName(Pid).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(SeqNr).keyType(KeyType.RANGE).build())

      val tableAttributes = Seq(
        AttributeDefinition.builder().attributeName(Pid).attributeType(ScalarAttributeType.S).build(),
        AttributeDefinition.builder().attributeName(SeqNr).attributeType(ScalarAttributeType.N).build())

      val tableWithIndexAttributes = tableAttributes ++ Seq(
        AttributeDefinition.builder().attributeName(EntityTypeSlice).attributeType(ScalarAttributeType.S).build(),
        AttributeDefinition.builder().attributeName(Timestamp).attributeType(ScalarAttributeType.N).build())

      requestBuilder = sliceIndex match {
        case Some(index) =>
          requestBuilder
            .attributeDefinitions(tableWithIndexAttributes: _*)
            .globalSecondaryIndexes(index)
        case None =>
          requestBuilder.attributeDefinitions(tableAttributes: _*)
      }

      requestBuilder = tableSettings.throughput match {
        case provisioned: ProvisionedThroughputSettings =>
          requestBuilder.provisionedThroughput(
            ProvisionedThroughput.builder
              .readCapacityUnits(provisioned.readCapacityUnits)
              .writeCapacityUnits(provisioned.writeCapacityUnits)
              .build())
        case onDemand: OnDemandThroughputSettings =>
          requestBuilder.onDemandThroughput(
            OnDemandThroughput.builder
              .maxReadRequestUnits(onDemand.maxReadRequestUnits)
              .maxWriteRequestUnits(onDemand.maxWriteRequestUnits)
              .build())
      }

      client
        .createTable(requestBuilder.build())
        .asScala
        .map(_ => Done)
        .recoverWith { case c: CompletionException =>
          Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
    }

    def delete(): Future[Done] = {
      val req = DeleteTableRequest.builder().tableName(settings.journalTable).build()
      client
        .deleteTable(req)
        .asScala
        .map(_ => Done)
        .recoverWith { case c: CompletionException =>
          Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
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
      deleteIfExists: Boolean,
      tableSettings: TableSettings = TableSettings.Local,
      sliceIndexSettings: IndexSettings = IndexSettings.Local): Future[Done] = {
    import akka.persistence.dynamodb.internal.SnapshotAttributes._

    implicit val ec: ExecutionContext = system.executionContext

    val existingTable =
      client.describeTable(DescribeTableRequest.builder().tableName(settings.snapshotTable).build()).asScala

    def create(): Future[Done] = {
      val sliceIndex = if (sliceIndexSettings.enabled) {
        var indexBuilder =
          GlobalSecondaryIndex.builder
            .indexName(settings.snapshotBySliceGsi)
            .keySchema(
              KeySchemaElement.builder().attributeName(EntityTypeSlice).keyType(KeyType.HASH).build(),
              KeySchemaElement.builder().attributeName(EventTimestamp).keyType(KeyType.RANGE).build())
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())

        indexBuilder = sliceIndexSettings.throughput match {
          case provisioned: ProvisionedThroughputSettings =>
            indexBuilder.provisionedThroughput(
              ProvisionedThroughput.builder
                .readCapacityUnits(provisioned.readCapacityUnits)
                .writeCapacityUnits(provisioned.writeCapacityUnits)
                .build())
          case onDemand: OnDemandThroughputSettings =>
            indexBuilder.onDemandThroughput(
              OnDemandThroughput.builder
                .maxReadRequestUnits(onDemand.maxReadRequestUnits)
                .maxWriteRequestUnits(onDemand.maxWriteRequestUnits)
                .build())
        }

        Some(indexBuilder.build())
      } else None

      var requestBuilder = CreateTableRequest.builder
        .tableName(settings.snapshotTable)
        .keySchema(KeySchemaElement.builder().attributeName(Pid).keyType(KeyType.HASH).build())

      val tableAttributes =
        Seq(AttributeDefinition.builder().attributeName(Pid).attributeType(ScalarAttributeType.S).build())

      val tableWithIndexAttributes = tableAttributes ++ Seq(
        AttributeDefinition.builder().attributeName(EntityTypeSlice).attributeType(ScalarAttributeType.S).build(),
        AttributeDefinition.builder().attributeName(EventTimestamp).attributeType(ScalarAttributeType.N).build())

      requestBuilder = sliceIndex match {
        case Some(index) =>
          requestBuilder
            .attributeDefinitions(tableWithIndexAttributes: _*)
            .globalSecondaryIndexes(index)
        case None =>
          requestBuilder.attributeDefinitions(tableAttributes: _*)
      }

      requestBuilder = tableSettings.throughput match {
        case provisioned: ProvisionedThroughputSettings =>
          requestBuilder.provisionedThroughput(
            ProvisionedThroughput.builder
              .readCapacityUnits(provisioned.readCapacityUnits)
              .writeCapacityUnits(provisioned.writeCapacityUnits)
              .build())
        case onDemand: OnDemandThroughputSettings =>
          requestBuilder.onDemandThroughput(
            OnDemandThroughput.builder
              .maxReadRequestUnits(onDemand.maxReadRequestUnits)
              .maxWriteRequestUnits(onDemand.maxWriteRequestUnits)
              .build())
      }

      client
        .createTable(requestBuilder.build())
        .asScala
        .map(_ => Done)
        .recoverWith { case c: CompletionException =>
          Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
    }

    def delete(): Future[Done] = {
      val req = DeleteTableRequest.builder().tableName(settings.snapshotTable).build()
      client
        .deleteTable(req)
        .asScala
        .map(_ => Done)
        .recoverWith { case c: CompletionException =>
          Future.failed(c.getCause)
        }(ExecutionContext.parasitic)
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
