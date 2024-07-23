# Query Plugin

## Event sourced queries

@apidoc[DynamoDBReadJournal] implements the following @extref:[Persistence Queries](akka:persistence-query.html):

* `eventsBySlices`, `currentEventsBySlices`

Accessing the `DynamoDBReadJournal`:

Java
:  @@snip [access read journal](/docs/src/test/java/docs/javadsl/QueryDocExample.java) { #access-read-journal }

Scala
:  @@snip [access read journal](/docs/src/test/scala/docs/scaladsl/QueryDocExample.scala) { #access-read-journal }

### eventsBySlices

The `eventsBySlices` and `currentEventsBySlices` queries are useful for retrieving all events for a given entity type.
`eventsBySlices` should be used via Akka Projection.

@@@ note
This has historically been done with `eventsByTag` but the DynamoDB plugin is instead providing `eventsBySlices`
as an improved solution.

The usage of `eventsByTag` for Projections has the drawback that the number of tags must be decided up-front and can't
easily be changed afterwards. Starting with too many tags means a lot of overhead, since many projection instances
would be running on each node in a small Akka Cluster, with each projection instance polling the database periodically.
Starting with too few tags means that it can't be scaled later to more Akka nodes.

With `eventsBySlices` more Projection instances can be added when needed and still reuse the offsets for the previous
slice distributions.
@@@

A slice is deterministically defined based on the persistence id. The purpose is to evenly distribute all
persistence ids over the slices. The `eventsBySlices` query is for a range of the slices. For example if
using 1024 slices and running 4 Projection instances the slice ranges would be 0-255, 256-511, 512-767, 768-1023.
Changing to 8 slice ranges means that the ranges would be 0-127, 128-255, 256-383, ..., 768-895, 896-1023.

Example of `currentEventsBySlices`:

Java
:  @@snip [create](/docs/src/test/java/docs/javadsl/QueryDocExample.java) { #current-events-by-slices }

Scala
:  @@snip [create](/docs/src/test/scala/docs/scaladsl/QueryDocExample.scala) { #current-events-by-slices }

`eventsBySlices` should be used via @ref:[DynamoDBProjection](projection.md), which will automatically handle some
challenges around ordering and missed events.

When using `DynamoDBProjection` the events will be delivered in sequence number order without duplicates.

The consumer can keep track of its current position in the event stream by storing the `offset` and restarting the
query from a given `offset` after a crash/restart.

The offset is a `TimestampOffset`, based on when the event was stored. This means that a "later" event may be visible
first. When retrieving events after the previously seen timestamp we may miss some events and emit an event with a
later sequence number for a persistence id, without emitting all preceding events. `eventsBySlices` will perform
additional backtracking queries to catch missed events. Events from backtracking will typically be duplicates of
previously emitted events. It's the responsibility of the consumer to filter duplicates and make sure that events are
processed in exact sequence number order for each persistence id, and such de-duplication is provided by the DynamoDB
Projection.

Events emitted by backtracking don't contain the event payload (`EventBySliceEnvelope.event` is None) and the consumer
can load the full `EventBySliceEnvelope` with @apidoc[DynamoDBReadJournal.loadEnvelope](DynamoDBReadJournal) {
scala="#loadEnvelope[Event](persistenceId:String,sequenceNr:Long):scala.concurrent.Future[akka.persistence.query.typed.EventEnvelope[Event]]"
java="#loadEnvelope[Event](persistenceId:String,sequenceNr:Long):java.util.concurrent.CompletionStage[akka.persistence.query.typed.EventEnvelope[Event]]"
}.

Events will be emitted in timestamp order with the caveat of duplicate events as described above. Events with the same
timestamp are ordered by sequence number.

`currentEventsBySlices` doesn't perform backtracking queries, will not emit duplicates, and the event payload is always
fully loaded.

@@@ note

The @ref:[journal table](journal.md#tables) must be created with a global secondary index to @ref:[index events
by slice](journal.md#indexes).

@@@

### eventsBySlicesStartingFromSnapshots

The `eventsBySlicesStartingFromSnapshots` and `currentEventsBySlicesStartingFromSnapshots` queries are like the
@ref:[eventsBySlices](#eventsbyslices) queries, but use snapshots as starting points to reduce the number of events
that need to be loaded. This can be useful if a consumer starts from zero without any previously processed offsets, or
if it has been disconnected for a long while and its offsets are far behind.

These queries first load all snapshots with timestamps greater than or equal to the offset timestamp. There is at most
one snapshot per persistenceId. The snapshots are transformed into events with the given `transformSnapshot` function.
After emitting the snapshot events, the ordinary events with sequence numbers after the snapshots are emitted.

To use the start-from-snapshot queries you must also enable configuration:

```
akka.persistence.dynamodb.query.start-from-snapshot.enabled = true
```

@@@ note

The @ref:[snapshot table](snapshots.md#tables) must be created with a global secondary index to @ref:[index snapshots
by slice](snapshots.md#indexes).

@@@

### Publish events for lower latency of eventsBySlices

The `eventsBySlices` query polls the database periodically to find new events. By default, this interval is a few
seconds, see `akka.persistence.dynamodb.query.refresh-interval` in the @ref:[Configuration](#configuration). This
interval can be reduced for lower latency, with the drawback of querying the database more frequently.

To reduce the latency there is a feature that will publish the events within the Akka Cluster. Running `eventsBySlices`
will subscribe to the events and emit them directly without waiting for the next query poll. The trade-off is that more
CPU and network resources are used. The events must still be retrieved from the database, but at a lower polling
frequency, because delivery of published messages are not guaranteed.

This feature is enabled by default and it will measure the throughput and automatically disable/enable if
the exponentially weighted moving average of measured throughput exceeds the configured threshold.

```
akka.persistence.dynamodb.journal.publish-events-dynamic.throughput-threshold = 400
```

Disable publishing of events with configuration:

```
akka.persistence.dynamodb.journal.publish-events = off
```

If you use many queries or Projection instances you should consider adjusting the
`akka.persistence.dynamodb.journal.publish-events-number-of-topics` configuration, see
@ref:[Configuration](#configuration).

## Configuration

Query configuration is under `akka.persistence.dynamodb.query`.

The query plugin shares the @ref:[DynamoDB client configuration](config.md#dynamodb-client-configuration) with other
plugins.

### Reference configuration

The following can be overridden in your `application.conf` for query specific settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#query-settings}

