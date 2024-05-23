# Getting Started

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

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

The plugin is published for Scala 2.13 and 2.12.

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

