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

TODO see r2dbc docs
