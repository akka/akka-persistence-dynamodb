
package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.typed.PersistenceId;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.producer.javadsl.EventProducer;
import akka.projection.grpc.producer.javadsl.EventProducerSource;
import akka.projection.grpc.producer.javadsl.Transformation;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class PublishEventsGrpc {

  public static Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerService(
      ActorSystem<?> system) {
    Transformation transformation =
        Transformation.empty() 
            .registerEnvelopeMapper(
                ShoppingCart.ItemAdded.class, envelope -> Optional.of(transformItemAdded(envelope)))
            .registerEnvelopeMapper(
                ShoppingCart.CheckedOut.class,
                envelope -> Optional.of(transformCheckedOut(envelope)))
            
            .registerEnvelopeMapper(
                ShoppingCart.ItemQuantityAdjusted.class,
                envelope -> Optional.of(transformItemQuantityAdjusted(envelope)))
        
        ;

    EventProducerSource eventProducerSource =
        new EventProducerSource(
            "ShoppingCart", 
            "cart", 
            transformation, 
            EventProducerSettings.create(system));

    return EventProducer.grpcServiceHandler(system, eventProducerSource);
  }

  

  
  private static shopping.cart.proto.ItemAdded transformItemAdded(
      EventEnvelope<ShoppingCart.ItemAdded> envelope) {
    var itemUpdated = envelope.event();
    return shopping.cart.proto.ItemAdded.newBuilder()
        .setCartId(PersistenceId.extractEntityId(envelope.persistenceId()))
        .setItemId(itemUpdated.itemId())
        .setQuantity(itemUpdated.quantity())
        .build();
  }

  private static shopping.cart.proto.CheckedOut transformCheckedOut(
      EventEnvelope<ShoppingCart.CheckedOut> envelope) {
    return shopping.cart.proto.CheckedOut.newBuilder()
        .setCartId(PersistenceId.extractEntityId(envelope.persistenceId()))
        .build();
  }

  

  private static shopping.cart.proto.ItemQuantityAdjusted transformItemQuantityAdjusted(
      EventEnvelope<ShoppingCart.ItemQuantityAdjusted> envelope) {
    var itemUpdated = envelope.event();
    return shopping.cart.proto.ItemQuantityAdjusted.newBuilder()
        .setCartId(PersistenceId.extractEntityId(envelope.persistenceId()))
        .setItemId(itemUpdated.itemId())
        .setQuantity(itemUpdated.newQuantity())
        .build();
  }

  
}

