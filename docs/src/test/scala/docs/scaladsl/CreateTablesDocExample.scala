/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Futures
import org.scalatest.wordspec.AnyWordSpecLike

//#create-tables
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.dynamodb.util.scaladsl.CreateTables
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

//#create-tables

object CreateTablesDocExample {
  val config: Config = ConfigFactory.load(ConfigFactory.parseString("""
    //#local-mode
    akka.persistence.dynamodb {
      client.local.enabled = true
    }
    //#local-mode
    """))
}

class CreateTablesDocExample
    extends ScalaTestWithActorTestKit(CreateTablesDocExample.config)
    with AnyWordSpecLike
    with Futures {

  "Getting started docs" should {

    "have example of creating tables locally (Scala)" in {
      //#create-tables
      val dynamoDBConfigPath = "akka.persistence.dynamodb"
      val dynamoDBClientConfigPath = dynamoDBConfigPath + ".client"

      val settings: DynamoDBSettings = DynamoDBSettings(system.settings.config.getConfig(dynamoDBConfigPath))
      val client: DynamoDbAsyncClient = ClientProvider(system).clientFor(dynamoDBClientConfigPath)

      // create journal table, synchronously
      Await.result(CreateTables.createJournalTable(system, settings, client, deleteIfExists = true), 10.seconds)

      // create snapshot table, synchronously
      Await.result(CreateTables.createSnapshotsTable(system, settings, client, deleteIfExists = true), 10.seconds)
      //#create-tables
    }

    "have example of creating tables locally (Java)" in {
      docs.javadsl.CreateTablesDocExample.createTables(system)
    }

  }
}
