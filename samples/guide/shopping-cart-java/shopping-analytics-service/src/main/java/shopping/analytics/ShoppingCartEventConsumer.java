
package shopping.analytics;

import static akka.Done.done;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.japi.Pair;
import akka.persistence.Persistence;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.dynamodb.javadsl.DynamoDBProjection;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.consumer.javadsl.GrpcReadJournal;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.proto.*;

class ShoppingCartEventConsumer {
  private static final Logger log = LoggerFactory.getLogger(ShoppingCartEventConsumer.class);

  

  
  static class EventHandler extends Handler<EventEnvelope<Object>> {
    private final ProjectionId projectionId;

    private long totalCount = 0;

    
    private long throughputStartTime = System.nanoTime();
    private int throughputCount = 0;

    

    EventHandler(ProjectionId projectionId) {
      this.projectionId = projectionId;
    }

    @Override
    public CompletionStage<Done> start() {
      log.info("Started Projection [{}].", projectionId.id());
      return super.start();
    }

    @Override
    public CompletionStage<Done> stop() {
      log.info("Stopped Projection [{}]. Consumed [{}] events.", projectionId.id(), totalCount);
      return super.stop();
    }

    @Override
    public CompletionStage<Done> process(EventEnvelope<Object> envelope) {
      Object event = envelope.getEvent();
      totalCount++;

      switch (event) {
        case ItemAdded itemAdded:
          log.info(
              "Projection [{}] consumed ItemAdded for cart {}, added {} {}. Total [{}] events.",
              projectionId.id(),
              itemAdded.getCartId(),
              itemAdded.getQuantity(),
              itemAdded.getItemId(),
              totalCount);
          break;
        case CheckedOut checkedOut:
          log.info(
              "Projection [{}] consumed CheckedOut for cart {}. Total [{}] events.",
              projectionId.id(),
              checkedOut.getCartId(),
              totalCount);
          break;
          
        case ItemQuantityAdjusted itemQuantityAdjusted:
          log.info(
              "Projection [{}] consumed ItemQuantityAdjusted for cart {}, changed {} {}. Total [{}] events.",
              projectionId.id(),
              itemQuantityAdjusted.getCartId(),
              itemQuantityAdjusted.getQuantity(),
              itemQuantityAdjusted.getItemId(),
              totalCount);
          break;
        case ItemRemoved itemRemoved:
          log.info(
              "Projection [{}] consumed ItemRemoved for cart {}, removed {}. Total [{}] events.",
              projectionId.id(),
              itemRemoved.getCartId(),
              itemRemoved.getItemId(),
              totalCount);
          break;
          
        default:
          throw new IllegalArgumentException("Unknown event " + event);
      }
      

      throughputCount++;
      long durationMs = (System.nanoTime() - throughputStartTime) / 1000 / 1000;
      if (throughputCount >= 1000 || durationMs >= 10000) {
        log.info(
            "Projection [{}] throughput [{}] events/s in [{}] ms",
            projectionId.id(),
            1000L * throughputCount / durationMs,
            durationMs);
        throughputCount = 0;
        throughputStartTime = System.nanoTime();
      }
      
      return CompletableFuture.completedFuture(done());
    }
  }

  

  
  public static void init(ActorSystem<?> system) {
    int numberOfProjectionInstances = 4; 
    String projectionName = "cart-events";
    List<Pair<Integer, Integer>> sliceRanges =
        Persistence.get(system).getSliceRanges(numberOfProjectionInstances); 

    GrpcReadJournal eventsBySlicesQuery =
        GrpcReadJournal.create(system, List.of(ShoppingCartEvents.getDescriptor()));

    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            projectionName,
            numberOfProjectionInstances,
            idx -> {
              Pair<Integer, Integer> sliceRange = sliceRanges.get(idx);
              String projectionKey =
                  eventsBySlicesQuery.streamId()
                      + "-"
                      + sliceRange.first()
                      + "-"
                      + sliceRange.second(); 
              ProjectionId projectionId = ProjectionId.of(projectionName, projectionKey);

              SourceProvider<Offset, EventEnvelope<Object>> sourceProvider =
                  EventSourcedProvider.eventsBySlices(
                      system,
                      eventsBySlicesQuery,
                      eventsBySlicesQuery.streamId(),
                      sliceRange.first(),
                      sliceRange.second());

              return ProjectionBehavior.create(
                  DynamoDBProjection.atLeastOnceAsync(
                      projectionId,
                      Optional.empty(),
                      sourceProvider,
                      () -> new EventHandler(projectionId), 
                      system));
            },
            ProjectionBehavior.stopMessage());
  }
}

