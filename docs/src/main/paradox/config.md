# Configuration

## DynamoDB client configuration

Configuration for how to connect to DynamoDB is located under `akka.persistence.dynamodb.client`.

The [default credentials provider](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) will be used for authentication.

A local mode can be enabled, for testing with a [DynamoDB local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) instance.

The following client settings be overridden in the `client` block:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#client-settings}

## Journal configuration

Journal configuration properties are by default defined under `akka.persistence.dynamodb.journal`.

See @ref:[Journal plugin configuration](journal.md#configuration).

## Snapshot configuration

Snapshot store configuration properties are by default defined under `akka.persistence.dynamodb.snapshot`.

See @ref:[Snapshot store plugin configuration](snapshots.md#configuration).

## Query configuration

Query configuration properties are by default defined under `akka.persistence.dynamodb.query`.

See @ref:[Query plugin configuration](query.md#configuration).

## Multiple plugins

To enable the DynamoDB plugins to be used by default, add the following lines to your `application.conf`:

@@snip [default plugins](/docs/src/test/scala/docs/scaladsl/MultiPluginDocExample.scala) { #default-config }

Note that all plugins have a shared root config section `akka.persistence.dynamodb`, which also contains the
@ref:[DynamoDB client configuration](#dynamodb-client-configuration).

You can use additional plugins with different configuration. For example, a second configuration could be defined:

@@snip [second config](/docs/src/test/scala/docs/scaladsl/MultiPluginDocExample.scala) { #second-config }

To use the additional plugin you would @scala[define]@java[override] the plugin id:

Java
: @@snip [with plugins](/docs/src/test/java/docs/javadsl/MultiPluginDocExample.java) { #with-plugins }

Scala
: @@snip [with plugins](/docs/src/test/scala/docs/scaladsl/MultiPluginDocExample.scala) { #with-plugins }

For queries and projection `SourceProvider` you would use `"second-dynamodb.query"` instead of the default
@scala[`DynamoDBReadJournal.Identifier`]@java[`DynamoDBReadJournal.Identifier()`]
(`"akka.persistence.dynamodb.query"`).

For additional details on multiple plugin configuration for projections see the @extref:[Akka Projection DynamoDB
docs](akka-projection:dynamodb.html#multiple-plugins).
