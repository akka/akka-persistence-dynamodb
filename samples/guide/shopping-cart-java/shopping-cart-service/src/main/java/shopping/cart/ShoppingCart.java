package shopping.cart;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.*;
import akka.serialization.jackson.CborSerializable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This is an event sourced actor (`EventSourcedBehavior`). An entity managed by Cluster Sharding.
 *
 * <p>It has a state, [[ShoppingCart.State]], which holds the current shopping cart items and
 * whether it's checked out.
 *
 * <p>You interact with event sourced actors by sending commands to them, see classes implementing
 * [[ShoppingCart.Command]].
 *
 * <p>The command handler validates and translates commands to events, see classes implementing
 * [[ShoppingCart.Event]]. It's the events that are persisted by the `EventSourcedBehavior`. The
 * event handler updates the current state based on the event. This is done when the event is first
 * created, and when the entity is loaded from the database - each event will be replayed to
 * recreate the state of the entity.
 */
public final class ShoppingCart
    extends EventSourcedBehaviorWithEnforcedReplies<
        ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State> {

  /** The current state held by the `EventSourcedBehavior`. */
  
  public record State(Map<String, Integer> items, Optional<Instant> checkoutDate)
      implements CborSerializable {

    public State() {
      this(new HashMap<>(), Optional.empty());
    }

    public boolean isCheckedOut() {
      return checkoutDate.isPresent();
    }

    public State checkout(Instant now) {
      return new State(items, Optional.of(now));
    }

    public Summary toSummary() {
      return new Summary(items, isCheckedOut());
    }

    public boolean hasItem(String itemId) {
      return items.containsKey(itemId);
    }

    public State updateItem(String itemId, int quantity) {
      Map<String, Integer> updatedItems = new HashMap<>(this.items);
      if (quantity == 0) {
        updatedItems.remove(itemId);
      } else {
        updatedItems.put(itemId, quantity);
      }
      return new State(updatedItems, this.checkoutDate);
    }

    public boolean isEmpty() {
      return items.isEmpty();
    }

    

    public State removeItem(String itemId) {
      Map<String, Integer> updatedItems = new HashMap<>(this.items);
      updatedItems.remove(itemId);
      return new State(updatedItems, this.checkoutDate);
    }

    public int itemCount(String itemId) {
      return items.get(itemId);
    }
  }

  

  /** This interface defines all the commands (messages) that the ShoppingCart actor supports. */
  public sealed interface Command extends CborSerializable {}

  /**
   * A command to add an item to the cart.
   *
   * <p>It replies with `StatusReply&lt;Summary&gt;`, which is sent back to the caller when all the
   * events emitted by this command are successfully persisted.
   */
  record AddItem(String itemId, int quantity, ActorRef<StatusReply<Summary>> replyTo)
      implements Command {}

  /** A command to remove an item from the cart. */
  record RemoveItem(String itemId, ActorRef<StatusReply<Summary>> replyTo) implements Command {}

  /** A command to adjust the quantity of an item in the cart. */
  record AdjustItemQuantity(String itemId, int quantity, ActorRef<StatusReply<Summary>> replyTo)
      implements Command {}

  /** A command to checkout the shopping cart. */
  record Checkout(ActorRef<StatusReply<Summary>> replyTo) implements Command {}

  /** A command to get the current state of the shopping cart. */
  record Get(ActorRef<Summary> replyTo) implements Command {}

  /** Summary of the shopping cart state, used in reply messages. */
  public record Summary(Map<String, Integer> items, boolean checkedOut)
      implements CborSerializable {}

  public sealed interface Event extends CborSerializable {
    String cartId();
  }

  record ItemAdded(String cartId, String itemId, int quantity) implements Event {}

  record ItemRemoved(String cartId, String itemId, int oldQuantity) implements Event {}

  record ItemQuantityAdjusted(String cartId, String itemId, int oldQuantity, int newQuantity)
      implements Event {}

  record CheckedOut(String cartId, Instant eventTime) implements Event {}

  static final EntityTypeKey<Command> ENTITY_KEY =
      EntityTypeKey.create(Command.class, "ShoppingCart");

  
  public static void init(ActorSystem<?> system) {
    ClusterSharding.get(system)
        .init(
            Entity.of(
                ENTITY_KEY, entityContext -> ShoppingCart.create(entityContext.getEntityId())));
  }

  

  public static Behavior<Command> create(String cartId) {
    return Behaviors.setup(ctx -> EventSourcedBehavior.start(new ShoppingCart(cartId), ctx));
  }

  private final String cartId;

  private ShoppingCart(String cartId) {
    super(
        PersistenceId.of(ENTITY_KEY.name(), cartId),
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
    this.cartId = cartId;
  }

  @Override
  public RetentionCriteria retentionCriteria() {
    return RetentionCriteria.snapshotEvery(100, 3);
  }

  @Override
  public State emptyState() {
    return new State();
  }

  @Override
  public CommandHandlerWithReply<Command, Event, State> commandHandler() {
    return openShoppingCart().orElse(checkedOutShoppingCart()).orElse(getCommandHandler()).build();
  }

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> openShoppingCart() {
    return newCommandHandlerWithReplyBuilder()
        .forState(state -> !state.isCheckedOut())
        .onCommand(AddItem.class, this::onAddItem)
        .onCommand(RemoveItem.class, this::onRemoveItem)
        .onCommand(AdjustItemQuantity.class, this::onAdjustItemQuantity)
        .onCommand(Checkout.class, this::onCheckout);
  }

  private ReplyEffect<Event, State> onAddItem(State state, AddItem cmd) {
    if (state.hasItem(cmd.itemId)) {
      return Effect()
          .reply(
              cmd.replyTo,
              StatusReply.error(
                  "Item '" + cmd.itemId + "' was already added to this shopping cart"));
    } else if (cmd.quantity <= 0) {
      return Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"));
    } else {
      return Effect()
          .persist(new ItemAdded(cartId, cmd.itemId, cmd.quantity))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    }
  }

  private ReplyEffect<Event, State> onCheckout(State state, Checkout cmd) {
    if (state.isEmpty()) {
      return Effect()
          .reply(cmd.replyTo, StatusReply.error("Cannot checkout an empty shopping cart"));
    } else {
      return Effect()
          .persist(new CheckedOut(cartId, Instant.now()))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    }
  }

  private ReplyEffect<Event, State> onRemoveItem(State state, RemoveItem cmd) {
    if (state.hasItem(cmd.itemId)) {
      return Effect()
          .persist(new ItemRemoved(cartId, cmd.itemId, state.itemCount(cmd.itemId)))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    } else {
      return Effect()
          .reply(
              cmd.replyTo,
              StatusReply.success(state.toSummary())); // removing an item is idempotent
    }
  }

  private ReplyEffect<Event, State> onAdjustItemQuantity(State state, AdjustItemQuantity cmd) {
    if (cmd.quantity <= 0) {
      return Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"));
    } else if (state.hasItem(cmd.itemId)) {
      return Effect()
          .persist(
              new ItemQuantityAdjusted(
                  cartId, cmd.itemId, state.itemCount(cmd.itemId), cmd.quantity))
          .thenReply(cmd.replyTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
    } else {
      return Effect()
          .reply(
              cmd.replyTo,
              StatusReply.error(
                  "Cannot adjust quantity for item '"
                      + cmd.itemId
                      + "'. Item not present on cart"));
    }
  }

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State>
      checkedOutShoppingCart() {
    return newCommandHandlerWithReplyBuilder()
        .forState(State::isCheckedOut)
        .onCommand(
            AddItem.class,
            cmd ->
                Effect()
                    .reply(
                        cmd.replyTo,
                        StatusReply.error(
                            "Can't add an item to an already checked out shopping cart")))
        .onCommand(
            RemoveItem.class,
            cmd ->
                Effect()
                    .reply(
                        cmd.replyTo,
                        StatusReply.error(
                            "Can't remove an item from an already checked out shopping cart")))
        .onCommand(
            AdjustItemQuantity.class,
            cmd ->
                Effect()
                    .reply(
                        cmd.replyTo,
                        StatusReply.error(
                            "Can't adjust item on an already checked out shopping cart")))
        .onCommand(
            Checkout.class,
            cmd ->
                Effect()
                    .reply(
                        cmd.replyTo,
                        StatusReply.error("Can't checkout already checked out shopping cart")));
  }

  private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> getCommandHandler() {
    return newCommandHandlerWithReplyBuilder()
        .forAnyState()
        .onCommand(Get.class, (state, cmd) -> Effect().reply(cmd.replyTo, state.toSummary()));
  }

  @Override
  public EventHandler<State, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(ItemAdded.class, (state, evt) -> state.updateItem(evt.itemId, evt.quantity))
        .onEvent(ItemRemoved.class, (state, evt) -> state.removeItem(evt.itemId))
        .onEvent(
            ItemQuantityAdjusted.class,
            (state, evt) -> state.updateItem(evt.itemId, evt.newQuantity))
        .onEvent(CheckedOut.class, (state, evt) -> state.checkout(evt.eventTime))
        .build();
  }
}
