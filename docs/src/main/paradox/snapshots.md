# Snapshot store plugin

The snapshot plugin enables storing and loading snapshots for
@extref:[event sourced persistent actors](akka-core:typed/persistence.html).

## Tables

The snapshot plugin requires a snapshot table to be created in DynamoDB. The default table name is `snapshot` and this
can be configured (see the @ref:[reference configuration](#reference-configuration) for all settings). The table should
be created with the following attributes and key schema:

| Attribute name | Attribute type | Key type |
| -------------- | -------------- | -------- |
| pid            | S (String)     | HASH     |

Read capacity units should be based on expected entity recoveries. Write capacity units should be based on expected
rates for persisting snapshots.

An example `aws` CLI command for creating the snapshot table:

@@snip [aws create snapshot table](/scripts/create-tables.sh) { #create-snapshot-table }

### Indexes

If @ref:[start-from-snapshot queries](query.md#eventsbyslicesstartingfromsnapshots) are being used, then a global
secondary index needs to be added to the snapshot table, to index snapshots by slice. The default name (derived from
the configured @ref:[table name](#tables)) for the secondary index is `snapshot_slice_idx` and may be explicitly set
(see the @ref:[reference configuration](#reference-configuration)). The following attribute definitions should be added to the snapshot table,
with key schema for the snapshot slice index:

| Attribute name    | Attribute type | Key type |
| ----------------- | -------------- | -------- |
| entity_type_slice | S (String)     | HASH     |
| event_timestamp   | N (Number)     | RANGE    |

Write capacity units for the index should be aligned with the snapshot table. Read capacity units should be based on
expected queries.

An example `aws` CLI command for creating the snapshot table and slice index:

@@snip [aws create snapshot table](/scripts/create-tables.sh) { #create-snapshot-table-with-slice-index }

### Creating tables locally

For creating tables with DynamoDB local for testing, see the
@ref:[CreateTables utility](getting-started.md#creating-tables-locally).

## Configuration

To enable the snapshot plugin to be used by default, add the following line to your Akka `application.conf`:

```
akka.persistence.snapshot-store.plugin = "akka.persistence.dynamodb.snapshot"
```

It can also be enabled with the `snapshotPluginId` for a specific `EventSourcedBehavior` and multiple plugin
configurations are supported.

@@@ note
Snapshots are optional, and if you know that the application doesn't store many events for each entity it is more
efficient to not enable the snapshot plugin, because then it will not try to read snapshots when recovering the entities.
@@@

### Reference configuration

The following can be overridden in your `application.conf` for the snapshot specific settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#snapshot-settings}

## Usage

The snapshot plugin is used whenever a snapshot write is triggered through the
@extref:[Akka Persistence APIs](akka-core:typed/persistence-snapshot.html).

## Snapshot serialization

The state is serialized with @extref:[Akka Serialization](akka-core:serialization.html) and the binary snapshot representation
is stored in the `snapshot` column together with information about what serializer that was used in the
`ser_id` and `ser_manifest` columns.

## Retention

The DynamoDB snapshot plugin only ever keeps *one* snapshot per persistence id in the database. If a `keepNSnapshots > 1`
is specified for an `EventSourcedBehavior` that setting will be ignored.
