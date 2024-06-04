package shopping.cart.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.ItemPopularity;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ItemPopularityRepositoryImpl implements ItemPopularityRepository {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final DynamoDbAsyncClient client;

  public ItemPopularityRepositoryImpl(DynamoDbAsyncClient client) {
    this.client = client;
  }

  @Override
  public CompletionStage<Long> saveOrUpdate(ItemPopularity itemPopularity) {
    return findById(itemPopularity.itemId()).thenCompose(existing -> {
      long newCount =
          existing.map(e -> e.count() + itemPopularity.count()).orElseGet(itemPopularity::count);

      Map<String, AttributeValue> attributes = new HashMap<>();
      attributes.put("item_id", AttributeValue.fromS(itemPopularity.itemId()));
      attributes.put("count", AttributeValue.fromN(String.valueOf(newCount)));

      // TODO this should use optimistic locking

      CompletableFuture<PutItemResponse> response = client.putItem(
          PutItemRequest.builder()
              .tableName("item_popularity")
              .item(attributes)
              .build());
      return response.thenApply(r -> newCount);
    });
  }

  @Override
  public CompletionStage<Optional<ItemPopularity>> findById(String id) {
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":item_id", AttributeValue.fromS(id));

    QueryRequest req = QueryRequest.builder()
        .tableName("item_popularity")
        .keyConditionExpression("item_id = :item_id")
        .expressionAttributeValues(expressionAttributeValues)
        .build();

    return client.query(req).thenApply(response -> {
      if (response.items().isEmpty()) {
        return Optional.empty();
      } else {
        long count = Long.valueOf(response.items().getFirst().get("count").n());
        return Optional.of(new ItemPopularity(id, count));
      }
    });
  }
}
