
package shopping.cart;

import akka.Done;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.javadsl.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.repository.ItemPopularityRepository;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class ItemPopularityProjectionHandler
    extends Handler<EventEnvelope<ShoppingCart.Event>> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String slice;
  private final ItemPopularityRepository repo;

  public ItemPopularityProjectionHandler(String slice, ItemPopularityRepository repo) {
    this.slice = slice;
    this.repo = repo;
  }

  private CompletionStage<ItemPopularity> findOrNew(String itemId) {
    return repo.findById(itemId)
        .thenApply(
            itemPopularity -> itemPopularity.orElseGet(() -> new ItemPopularity(itemId, 0L)));
  }

  @Override
  public CompletionStage<Done> process(EventEnvelope<ShoppingCart.Event> envelope) {
    ShoppingCart.Event event = envelope.event();

    switch (event) {
      case ShoppingCart.ItemAdded(var __, String itemId, int qtd) -> {
        var itemPopularity = new ItemPopularity(itemId, qtd);
        var updated = repo.saveOrUpdate(itemPopularity);
        return updated.thenApply(
            rows -> {
              logCount(itemId, rows);
              return Done.getInstance();
            });

      }
      case ShoppingCart.ItemQuantityAdjusted(var __, String itemId, int oldQtd, int newQtd) -> {
        return findOrNew(itemId)
            .thenApply(itemPop -> itemPop.changeCount(newQtd - oldQtd))
            .thenCompose(repo::saveOrUpdate)
            .thenApply(
                rows -> {
                  logCount(itemId, rows);
                  return Done.getInstance();
                });
      }
      case ShoppingCart.ItemRemoved(var __, String itemId, int oldQtd) -> {
        return findOrNew(itemId)
            .thenApply(itemPop -> itemPop.changeCount(-oldQtd))
            .thenCompose(itm -> repo.saveOrUpdate(itm))
            .thenApply(
                rows -> {
                  logCount(itemId, rows);
                  return Done.getInstance();
                });

      }
      case ShoppingCart.CheckedOut ignored -> {
        return CompletableFuture.completedFuture(Done.getInstance());
      }
      case null, default -> {
        throw new IllegalArgumentException("Unknown event type: " + event.getClass());
      }
    }
  }

  private void logCount(String itemId, Long count) {
    logger.info(
        "ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]",
        this.slice,
        itemId,
        count);
  }
}

