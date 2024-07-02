/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.scaladsl

import java.util.concurrent.CompletionException

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.FutureConverters._
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.projection.dynamodb.DynamoDBProjectionSettings
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

object CreateTables {
  def createTimestampOffsetStoreTable(
      system: ActorSystem[_],
      settings: DynamoDBProjectionSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean): Future[Done] = {
    import akka.projection.dynamodb.internal.OffsetStoreDao.OffsetStoreAttributes._
    implicit val ec: ExecutionContext = system.executionContext

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

      client
        .createTable(req)
        .asScala
        .map(_ => Done)
        .recoverWith { case c: CompletionException =>
          Future.failed(c.getCause)
        }(ExecutionContexts.parasitic)
    }

    def delete(): Future[Done] = {
      val req = DeleteTableRequest.builder().tableName(settings.timestampOffsetTable).build()
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
      case Failure(exc) =>
        Future.failed[Done](exc)
    }
  }

}
