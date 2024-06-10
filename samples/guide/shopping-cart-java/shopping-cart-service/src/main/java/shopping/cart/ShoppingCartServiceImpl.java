package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.grpc.GrpcServiceException;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.proto.*;
import shopping.cart.repository.ItemPopularityRepository;

public final class ShoppingCartServiceImpl implements ShoppingCartService {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final Duration timeout;
  private final ClusterSharding sharding;

  
  private final ItemPopularityRepository repository;

  private final ActorSystem<?> sys;

  public ShoppingCartServiceImpl(
      ActorSystem<?> system, ItemPopularityRepository repository) { 

    this.sys = system;
    this.repository = repository;
    timeout = system.settings().config().getDuration("shopping-cart-service.ask-timeout");
    sharding = ClusterSharding.get(system);
  }

  

  @Override
  public CompletionStage<Cart> addItem(AddItemRequest in) {
    logger.info("addItem {} to cart {}", in.getItemId(), in.getCartId());
    var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    CompletionStage<ShoppingCart.Summary> reply =
        entityRef.askWithStatus(
            replyTo -> new ShoppingCart.AddItem(in.getItemId(), in.getQuantity(), replyTo),
            timeout);
    var cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart);
    return convertError(cart);
  }

  @Override
  public CompletionStage<Cart> removeItem(RemoveItemRequest in) {
    logger.info("updateItem {}", in.getCartId());
    var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    CompletionStage<ShoppingCart.Summary> reply =
        entityRef.askWithStatus(
            replyTo -> new ShoppingCart.RemoveItem(in.getItemId(), replyTo), timeout);
    var cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart);
    return convertError(cart);
  }

  @Override
  public CompletionStage<Cart> updateItem(UpdateItemRequest in) {
    logger.info("getCart {}", in.getCartId());
    var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    final CompletionStage<ShoppingCart.Summary> reply;
    if (in.getQuantity() == 0) {
      reply =
          entityRef.askWithStatus(
              replyTo -> new ShoppingCart.RemoveItem(in.getItemId(), replyTo), timeout);
    } else {
      reply =
          entityRef.askWithStatus(
              replyTo ->
                  new ShoppingCart.AdjustItemQuantity(in.getItemId(), in.getQuantity(), replyTo),
              timeout);
    }
    var cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart);
    return convertError(cart);
  }

  
  @Override
  public CompletionStage<Cart> checkout(CheckoutRequest in) {
    logger.info("checkout {}", in.getCartId());
    var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    var reply =
        entityRef
            .askWithStatus(ShoppingCart.Checkout::new, timeout)
            .thenApply(ShoppingCartServiceImpl::toProtoCart);
    return convertError(reply);
  }

  @Override
  public CompletionStage<Cart> getCart(GetCartRequest in) {
    logger.info("getCart {}", in.getCartId());
    var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    var reply = entityRef.ask(ShoppingCart.Get::new, timeout);
    var protoCart =
        reply.thenApply(
            cart -> {
              if (cart.items().isEmpty())
                throw new GrpcServiceException(
                    Status.NOT_FOUND.withDescription("Cart " + in.getCartId() + " not found"));
              else return toProtoCart(cart);
            });
    return convertError(protoCart);
  }

  

  
  @Override
  public CompletionStage<GetItemPopularityResponse> getItemPopularity(GetItemPopularityRequest in) {

    CompletionStage<Optional<ItemPopularity>> itemPopularity = repository.findById(in.getItemId());

    return itemPopularity.thenApply(
        popularity -> {
          long count = popularity.map(ItemPopularity::count).orElse(0L);
          return GetItemPopularityResponse.newBuilder().setPopularityCount(count).build();
        });
  }

  

  
  private static Cart toProtoCart(ShoppingCart.Summary cart) {
    List<Item> protoItems =
        cart.items().entrySet().stream()
            .map(
                entry ->
                    Item.newBuilder()
                        .setItemId(entry.getKey())
                        .setQuantity(entry.getValue())
                        .build())
            .collect(Collectors.toList());

    return Cart.newBuilder().setCheckedOut(cart.checkedOut()).addAllItems(protoItems).build();
  }

  

  private static <T> CompletionStage<T> convertError(CompletionStage<T> response) {
    return response.exceptionally(
        ex -> {
          if (ex instanceof TimeoutException) {
            throw new GrpcServiceException(
                Status.UNAVAILABLE.withDescription("Operation timed out"));
          } else {
            throw new GrpcServiceException(Status.UNKNOWN.withDescription(ex.getMessage()));
          }
        });
  }
}
