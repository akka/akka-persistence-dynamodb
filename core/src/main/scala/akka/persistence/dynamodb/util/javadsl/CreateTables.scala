/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.util.javadsl

import java.util.concurrent.CompletionStage

import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.util.IndexSettings
import akka.persistence.dynamodb.util.TableSettings
import akka.persistence.dynamodb.util.scaladsl
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object CreateTables {
  def createJournalTable(
      system: ActorSystem[_],
      settings: DynamoDBSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean): CompletionStage[Done] =
    createJournalTable(system, settings, client, deleteIfExists, TableSettings.Local, IndexSettings.Local)

  def createJournalTable(
      system: ActorSystem[_],
      settings: DynamoDBSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean,
      tableSettings: TableSettings,
      sliceIndexSettings: IndexSettings): CompletionStage[Done] =
    scaladsl.CreateTables
      .createJournalTable(system, settings, client, deleteIfExists, tableSettings, sliceIndexSettings)
      .asJava

  def createSnapshotsTable(
      system: ActorSystem[_],
      settings: DynamoDBSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean): CompletionStage[Done] =
    createSnapshotsTable(system, settings, client, deleteIfExists, TableSettings.Local, IndexSettings.Local)

  def createSnapshotsTable(
      system: ActorSystem[_],
      settings: DynamoDBSettings,
      client: DynamoDbAsyncClient,
      deleteIfExists: Boolean,
      tableSettings: TableSettings,
      sliceIndexSettings: IndexSettings): CompletionStage[Done] =
    scaladsl.CreateTables
      .createSnapshotsTable(system, settings, client, deleteIfExists, tableSettings, sliceIndexSettings)
      .asJava
}
