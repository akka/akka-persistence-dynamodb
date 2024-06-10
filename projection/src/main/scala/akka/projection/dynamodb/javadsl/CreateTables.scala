/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.javadsl

import java.util.concurrent.CompletionStage

import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.scaladsl
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object CreateTables {
  def createTimestampOffsetStoreTable(
      system: ActorSystem[_],
      settings: DynamoDBProjectionSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean): CompletionStage[Done] =
    scaladsl.CreateTables.createTimestampOffsetStoreTable(system, settings, client, deleteIfExists).asJava

}
