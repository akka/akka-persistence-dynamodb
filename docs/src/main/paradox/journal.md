# Journal plugin

The journal plugin enables storing and loading events for
@extref:[event sourced persistent actors](akka:typed/persistence.html).

## Schema

TODO not schema, but maybe table provisioning and such

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

TODO
