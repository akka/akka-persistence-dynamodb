/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.util.ClientProvider
import akka.projection.dynamodb.scaladsl.CreateTables
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

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

  lazy val localDynamoDB = ClientProvider(typedSystem).clientSettingsFor(settings.useClient).local.isDefined

  private lazy val log = LoggerFactory.getLogger(getClass)

  override protected def beforeAll(): Unit = {
    if (localDynamoDB) {
      try {
        Await.result(
          akka.persistence.dynamodb.util.scaladsl.CreateTables
            .createJournalTable(typedSystem, dynamoDBSettings, client, deleteIfExists = true),
          10.seconds)
        Await.result(
          CreateTables.createTimestampOffsetStoreTable(typedSystem, settings, client, deleteIfExists = true),
          10.seconds)
      } catch {
        case NonFatal(ex) => throw new RuntimeException(s"Test db creation failed", ex)
      }
    }

    super.beforeAll()
  }

}
