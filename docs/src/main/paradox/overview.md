# Overview

The Akka Persistence DynamoDB plugin allows for using Amazon DynamoDB as a backend for Akka Persistence.

## Project Info

@@project-info{ projectId="core" }

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

@@dependencies{ projectId="core" }
