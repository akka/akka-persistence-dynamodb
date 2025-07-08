# Storing large state

DynamoDB imposes a limit on the sizes of objects being saved of 400 KiB (including metadata).  For the @ref:[journal](journal.md) and @ref:[snapshot store](snapshots.md) this imposes a limit on the serialized size of events or persistent actor states that is no greater than 300 KiB.

Regardless of the persistence plugin being used, it is generally a good idea to limit how much state one persistent actor is responsible for.  Ideally, a persistent actor is only responsible for state which requires strong consistency: if there is some portion of the state which does not have to be updated with the knowledge of the latest value of any other portion of the state, those two portions do not need to be the responsibility of the same actor (even if those portions are associated with the same "real-world" object being modeled).  However, there can be cases where consistency requirements are such that actors with responsibility for a lot of state are required (or at least the "least bad" option).

In order to allow large events (or even large batches of events, in terms of total write size) and state snapshots to be persisted, this plugin has support for defining a fallback store, with an implementation of a fallback store which uses [AWS S3](https://aws.amazon.com/s3/).

## S3 Fallback Overview

The S3 Fallback Store is under active development subject to revision.

### Project Info

@@project-info{ projectId="s3Fallback" }

### Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependency as below.

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-dynamodb-s3-fallback-store_$scala.binary.version$
  version=$project.version$
}

This plugin depends on Akka $akka.version$ or later, and note that it is important that all `akka-*` 
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems 
with transient dependencies causing an unlucky mix of versions.

@@dependencies{ projectId="s3Fallback" }

## Using S3 Fallback

The S3 Fallback store may be used for storing events or snapshots where the estimated size of the write (for events, this may be a batch of events) exceeds a configurable threshold.

To enable S3 Fallback for the journal, set `akka.persistence.dynamodb.journal.fallback-store.plugin = "akka.persistence.s3-fallback-store"` in your `application.conf`.

@@snip [application.conf](/s3-fallback-store/src/test/scala/docs/ConfigDocSpec.scala) { #enable-for-events }

To enable S3 Fallback for the snapshot store, set `akka.persistence.dynamodb.snapshot.fallback-store.plugin = "akka.persistence.s3-fallback-store"` in your `application.conf`.

@@snip [application.conf](/s3-fallback-store/src/test/scala/docs/ConfigDocSpec.scala) { #enable-for-snapshots }

If using S3 Fallback for both snapshots and events, the same bucket may be used.

## Configuration

The [default credentials provider](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) will be used for authentication.

The same HTTP client settings may be used for DynamoDB and S3: however the actual HTTP clients (including connection pools) will be separate.

### Reference configuration

@@snip [reference.conf](/s3-fallback-store/src/main/resources/reference.conf) {#fallback-settings}
