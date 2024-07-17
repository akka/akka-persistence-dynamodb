package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import akka.persistence.dynamodb.DynamoDBSettings;
import akka.persistence.dynamodb.util.ClientProvider;
import akka.projection.dynamodb.DynamoDBProjectionSettings;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.proto.ShoppingCartService;
import shopping.cart.repository.ItemPopularityRepository;
import shopping.cart.repository.ItemPopularityRepositoryImpl;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "shopping-cart-service");
    try {
      init(system);
    } catch (Exception e) {
      logger.error("Terminating due to initialization failure.", e);
      system.terminate();
    }
  }

  public static void init(ActorSystem<Void> system) {
    AkkaManagement.get(system).start();
    ClusterBootstrap.get(system).start();

    ShoppingCart.init(system);

    DynamoDbAsyncClient dynamoDBClient = ClientProvider.get(system).clientFor("akka.persistence.dynamodb.client");

    createTables(system, dynamoDBClient);

    ItemPopularityRepository itemPopularityRepository = new ItemPopularityRepositoryImpl(dynamoDBClient);
    ItemPopularityProjection.init(system, itemPopularityRepository);


    Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerService =
        PublishEventsGrpc.eventProducerService(system);


    Config config = system.settings().config();
    String grpcInterface = config.getString("shopping-cart-service.grpc.interface");
    int grpcPort = config.getInt("shopping-cart-service.grpc.port");
    ShoppingCartService grpcService = new ShoppingCartServiceImpl(system, itemPopularityRepository);
    ShoppingCartServer.start(grpcInterface, grpcPort, system, grpcService, eventProducerService);
  }

  public static void createTables(ActorSystem<Void> system, DynamoDbAsyncClient client) {
    try {
      akka.persistence.dynamodb.util.javadsl.CreateTables.createJournalTable(
              system, DynamoDBSettings.create(system), client, false)
          .toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.info(e.toString());
      // FIXME could be better error handling
      // continue anyway, table may exist
    }

    try {
      akka.persistence.dynamodb.util.javadsl.CreateTables.createSnapshotsTable(
                      system, DynamoDBSettings.create(system), client, false)
              .toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.info(e.toString());
      // FIXME could be better error handling
      // continue anyway, table may exist
    }

    try {
      akka.projection.dynamodb.javadsl.CreateTables.createTimestampOffsetStoreTable(
              system, DynamoDBProjectionSettings.create(system), client, false)
          .toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.info(e.toString());
      // FIXME could be better error handling
      // continue anyway, table may exist
    }


    try {
      CreateTableRequest itemPopularityTable = CreateTableRequest
          .builder()
          .tableName("item_popularity")
          .keySchema(
              KeySchemaElement.builder().attributeName("item_id").keyType(KeyType.HASH).build())
          .attributeDefinitions(
              AttributeDefinition.builder().attributeName("item_id").attributeType(ScalarAttributeType.S).build())
          .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build())
          .build();
      client.createTable(itemPopularityTable).toCompletableFuture().get(10, TimeUnit.SECONDS);

    } catch (Exception e) {
      logger.info(e.toString());
      // FIXME could be better error handling
      // continue anyway, table may exist
    }
  }
}
