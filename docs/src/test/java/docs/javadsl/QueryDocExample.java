/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import java.util.List;

import akka.actor.typed.ActorSystem;

// #access-read-journal
import akka.persistence.dynamodb.query.javadsl.DynamoDBReadJournal;
import akka.persistence.query.PersistenceQuery;

// #access-read-journal

// #current-events-by-slices
import akka.NotUsed;
import akka.japi.Pair;
import akka.persistence.query.NoOffset;
import akka.persistence.query.typed.EventEnvelope;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

// #current-events-by-slices

public class QueryDocExample {

  interface MyEvent {}

  public static DynamoDBReadJournal accessReadJournal(ActorSystem<?> system) {
    // #access-read-journal
    DynamoDBReadJournal eventQueries =
        PersistenceQuery.get(system)
            .getReadJournalFor(DynamoDBReadJournal.class, DynamoDBReadJournal.Identifier());
    // #access-read-journal

    return eventQueries;
  }

  void currentEventsBySlicesExampleCompileOnly(ActorSystem<?> system) {
    DynamoDBReadJournal eventQueries = accessReadJournal(system);

    // #current-events-by-slices
    // Split the slices into 4 ranges
    int numberOfSliceRanges = 4;
    List<Pair<Integer, Integer>> sliceRanges = eventQueries.sliceRanges(numberOfSliceRanges);

    // Example of using the first slice range
    int minSlice = sliceRanges.get(0).first();
    int maxSlice = sliceRanges.get(0).second();
    String entityType = "MyEntity";
    Source<EventEnvelope<MyEvent>, NotUsed> source =
        eventQueries.currentEventsBySlices(entityType, minSlice, maxSlice, NoOffset.getInstance());
    source
        .map(
            envelope ->
                "event from persistenceId "
                    + envelope.persistenceId()
                    + " with seqNr "
                    + envelope.sequenceNr()
                    + ": "
                    + envelope.event())
        .runWith(Sink.foreach(System.out::println), system);
    // #current-events-by-slices
  }
}
