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

@@snip [aws create event journal table](/scripts/create-tables.sh) { #create-event-journal-table }

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

<!-- TODO: ## Deletes -->
