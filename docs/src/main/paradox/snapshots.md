# Snapshot store plugin

The snapshot plugin enables storing and loading snapshots for
@extref:[event sourced persistent actors](akka:typed/persistence.html).

## Schema

TODO not schema, but maybe table provisioning and such

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
@extref:[Akka Persistence APIs](akka:typed/persistence-snapshot.html).

## Snapshot serialization

The state is serialized with @extref:[Akka Serialization](akka:serialization.html) and the binary snapshot representation
is stored in the `snapshot` column together with information about what serializer that was used in the
`ser_id` and `ser_manifest` columns.

## Retention

The DynamoDB snapshot plugin only ever keeps *one* snapshot per persistence id in the database. If a `keepNSnapshots > 1`
is specified for an `EventSourcedBehavior` that setting will be ignored.
