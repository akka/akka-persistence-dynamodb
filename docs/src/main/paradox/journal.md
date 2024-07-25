# Journal plugin

The journal plugin enables storing and loading events for
@extref:[event sourced persistent actors](akka:typed/persistence.html).

## Tables

The journal plugin requires an event journal table to be created in DynamoDB. The default table name is `event_journal`
and this can be configured (see the @ref:[reference configuration](#reference-configuration) for all settings). The
table should be created with the following attributes and key schema:

| Attribute name | Attribute type | Key type |
| -------------- | -------------- | -------- |
| pid            | S (String)     | HASH     |
| seq_nr         | N (Number)     | RANGE    |

Read capacity units should be based on expected entity recoveries. Write capacity units should be based on expected
rates for persisting events.

An example `aws` CLI command for creating the event journal table:

@@snip [aws create event journal table](/scripts/create-tables.sh) { #create-event-journal-table }

### Indexes

If @ref:[queries](query.md) or @ref:[projections](projection.md) are being used, then a global secondary index needs to
be added to the event journal table, to index events by slice. The default name for the secondary index is
`event_journal_slice_idx`. The following attribute definitions should be added to the event journal table, with key
schema for the event journal slice index:

| Attribute name    | Attribute type | Key type |
| ----------------- | -------------- | -------- |
| entity_type_slice | S (String)     | HASH     |
| ts                | N (Number)     | RANGE    |

Write capacity units for the index should be aligned with the event journal. Read capacity units should be based on
expected queries.

An example `aws` CLI command for creating the event journal table and slice index:

@@snip [aws create event journal table](/scripts/create-tables.sh) { #create-event-journal-table-with-slice-index }

### Creating tables locally

For creating tables with DynamoDB local for testing, see the
@ref:[CreateTables utility](getting-started.md#creating-tables-locally).

## Configuration

To enable the journal plugin to be used by default, add the following line to your Akka `application.conf`:

```
akka.persistence.journal.plugin = "akka.persistence.dynamodb.journal"
```

It can also be enabled with the `journalPluginId` for a specific `EventSourcedBehavior` and multiple plugin
configurations are supported.

### Reference configuration

The following can be overridden in your `application.conf` for the journal specific settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#journal-settings}

## Event serialization

The events are serialized with @extref:[Akka Serialization](akka:serialization.html) and the binary representation
is stored in the `event_payload` column together with information about what serializer that was used in the
`event_ser_id` and `event_ser_manifest` columns.

## Deletes

The journal supports deletes through hard deletes, which means that journal entries are actually deleted from the
database. There is no materialized view with a copy of the event, so make sure to not delete events too early if they
are used from projections or queries. A projection can also @ref:[start or continue from a
snapshot](query.md#eventsbyslicesstartingfromsnapshots), and then events can be deleted before the snapshot.

For each persistent id, a tombstone record is kept in the event journal when all events of a persistence id have been
deleted. The reason for the tombstone record is to keep track of the latest sequence number so that subsequent events
don't reuse the same sequence numbers that have been deleted.

See the @ref[EventSourcedCleanup tool](cleanup.md#event-sourced-cleanup-tool) for more information about how to delete
events, snapshots, and tombstone records.

### Time to Live (TTL)

Rather than deleting items immediately, the @ref[EventSourcedCleanup tool](cleanup.md#event-sourced-cleanup-tool) can
also be used to set an expiration timestamp on events or snapshots. DynamoDB's [Time to Live (TTL)][ttl] feature can
then be enabled, to automatically delete items after they have expired.

[ttl]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
