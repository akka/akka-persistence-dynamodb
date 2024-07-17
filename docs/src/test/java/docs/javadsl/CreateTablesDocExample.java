/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import java.util.concurrent.TimeUnit;

import akka.actor.typed.ActorSystem;

// #create-tables
import akka.persistence.dynamodb.DynamoDBSettings;
import akka.persistence.dynamodb.util.ClientProvider;
import akka.persistence.dynamodb.util.javadsl.CreateTables;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

// #create-tables

public class CreateTablesDocExample {

  public static void createTables(ActorSystem<?> system) throws Exception {
    // #create-tables
    String dynamoDBConfigPath = "akka.persistence.dynamodb";
    String dynamoDBClientConfigPath = dynamoDBConfigPath + ".client";

    DynamoDBSettings settings =
        DynamoDBSettings.create(system.settings().config().getConfig(dynamoDBConfigPath));

    DynamoDbAsyncClient client = ClientProvider.get(system).clientFor(dynamoDBClientConfigPath);

    // create journal table, synchronously
    CreateTables.createJournalTable(system, settings, client, /*deleteIfExists:*/ true)
        .toCompletableFuture()
        .get(10, TimeUnit.SECONDS);

    // create snapshot table, synchronously
    CreateTables.createSnapshotsTable(system, settings, client, /*deleteIfExists:*/ true)
        .toCompletableFuture()
        .get(10, TimeUnit.SECONDS);
    // #create-tables
  }

}
