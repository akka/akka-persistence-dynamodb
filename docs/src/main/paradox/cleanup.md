# Database cleanup

## Event Sourced cleanup tool

If possible, it is best to keep all events in an event sourced system. That way new @ref:[Projections](projection.md)
can be re-built. A @ref[Projection can also start or continue from a
snapshot](query.md#eventsbyslicesstartingfromsnapshots), and then events can be deleted before the snapshot.

In some cases keeping all events is not possible, or data must be removed for regulatory reasons, such as compliance
with GDPR. `EventSourcedBehavior`s can also automatically
@extref:[delete events on snapshot](akka:typed/persistence-snapshot.html#event-deletion). Snapshotting is useful even
if events aren't deleted as it speeds up recovery.

Deleting all events immediately when an entity has reached its terminal deleted state would have the consequence that
Projections may not have consumed all previous events yet and will not be notified of the deleted event. Instead, it's
recommended to emit a final deleted event and store the fact that the entity is deleted separately via a Projection.
Then a background task can clean up the events and snapshots for the deleted entities by using the
@apidoc[EventSourcedCleanup] tool. The entity itself knows about the terminal state from the deleted event and should
not emit further events after that, and typically stop itself if it receives any further commands.

Rather than deleting immediately, the @apidoc[EventSourcedCleanup] tool can also be used to set an expiration timestamp
on events or snapshots. DynamoDB's [Time to Live (TTL)][ttl] feature can then be enabled, to automatically delete items
after they have expired. The TTL attribute to use for events or snapshots is named `expiry`.

[ttl]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html

@apidoc[EventSourcedCleanup] operations include:

* Delete or set expiry for all events and snapshots for one or many persistence ids
* Delete or set expiry for all events for one or many persistence ids
* Delete or set expiry for all snapshots for one or many persistence ids
* Delete or set expiry for events before snapshot for one or many persistence ids
<!-- TODO: * Delete events before a timestamp -->

@@@ warning

When running an operation with `EventSourcedCleanup` that deletes all events for a persistence id, the actor with that
persistence id must not be running! If the actor is restarted, it would be recovered to the wrong state, since the
stored events have been deleted. Deleting events before snapshot can still be used while the actor is running.

@@@

<!-- TODO: current persistence ids queries not currently supported. -->
<!-- The cleanup tool can be combined with the @ref[query plugin](./query.md) which has a query to get all persistence ids. -->

## Reference configuration

The following can be overridden in your `application.conf` for cleanup specific settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #cleanup-settings }
