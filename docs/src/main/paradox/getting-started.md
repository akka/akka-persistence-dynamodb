# Getting Started

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL
for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependency as below.

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-dynamodb_$scala.binary.version$
  version=$project.version$
}

This plugin depends on Akka $akka.version$ or later, and note that it is important that all `akka-*` 
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems 
with transient dependencies causing an unlucky mix of versions.

The plugin is published for Scala 2.13 and 3.3.

## Enabling

To enable the plugins to be used by default, add the following line to your Akka `application.conf`:

```
akka.persistence.journal.plugin = "akka.persistence.dynamodb.journal"
akka.persistence.snapshot-store.plugin = "akka.persistence.dynamodb.snapshot"
```

More information about each individual plugin in:

* @ref:[journal](journal.md)
* @ref:[snapshot store](snapshots.md)
* @ref:[queries](query.md)

## Local testing with docker

[DynamoDB local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) can be run in
Docker. Here's an example docker compose file:

@@snip [docker-compose.yml](/docker/docker-compose.yml)

Start with:

```
docker compose -f docker/docker-compose.yml up
```

### Client local mode

The DynamoDB client @ref:[can be configured](config.md#dynamodb-client-configuration) with a local mode, for testing
with DynamoDB local:

@@snip [local mode](/docs/src/test/scala/docs/scaladsl/CreateTablesDocExample.scala) { #local-mode }

### Creating tables locally

A @apidoc[akka.persistence.dynamodb.util.*.CreateTables$] utility is provided for creating tables locally:

Java
: @@snip [create tables](/docs/src/test/java/docs/javadsl/CreateTablesDocExample.java) { #create-tables }

Scala
: @@snip [create tables](/docs/src/test/scala/docs/scaladsl/CreateTablesDocExample.scala) { #create-tables }

See the table definitions in the individual plugins for more information on the tables that are required:

* @ref:[journal tables](journal.md#tables)
* @ref:[snapshot tables](snapshots.md#tables)
* @extref:[projection tables](akka-projection:dynamodb.html#tables)
