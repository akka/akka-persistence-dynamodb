/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {
  val Scala213 = "2.13.15"
  val Scala3 = "3.3.3"
  val Scala2Versions = Seq(Scala213)
  val ScalaVersions = Dependencies.Scala2Versions :+ Dependencies.Scala3
  val AkkaVersion = System.getProperty("override.akka.version", "2.10.0")
  val AkkaVersionInDocs = VersionNumber(AkkaVersion).numbers match { case Seq(major, minor, _*) => s"$major.$minor" }

  // only for docs
  val AkkaProjectionVersion = "1.6.0"
  val AkkaProjectionVersionInDocs = VersionNumber(AkkaProjectionVersion).numbers match {
    case Seq(major, minor, _*) => s"$major.$minor"
  }
  val AwsSdkVersion = "2.29.41"
  // Java Platform version for JavaDoc creation
  // sync with Java version in .github/workflows/publish.yml#documentation
  val JavaDocLinkVersion = 17

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion
    val dynamodbSdk = "software.amazon.awssdk" % "dynamodb" % AwsSdkVersion

  }

  object TestDeps {
    val akkaStreamTyped = "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion % Test
    val akkaPersistenceTyped = Compile.akkaPersistenceTyped % Test
    val akkaShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion % Test
    val akkaPersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test
    val akkaTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
    val akkaJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion % Test

    val logback = "ch.qos.logback" % "logback-classic" % "1.5.8" % Test // EPL 1.0 / LGPL 2.1
    val scalaTest = "org.scalatest" %% "scalatest" % "3.2.12" % Test // ApacheV2
    val junit = "junit" % "junit" % "4.12" % Test // Eclipse Public License 1.0
    val junitInterface = "com.novocode" % "junit-interface" % "0.11" % Test // "BSD 2-Clause"

    val cloudwatchMetricPublisher = "software.amazon.awssdk" % "cloudwatch-metric-publisher" % AwsSdkVersion % Test
  }

  import Compile._

  val core = Seq(
    dynamodbSdk.exclude("software.amazon.awssdk", "apache-client"),
    akkaPersistence,
    akkaPersistenceQuery,
    TestDeps.akkaPersistenceTck,
    TestDeps.akkaStreamTestkit,
    TestDeps.akkaTestkit,
    TestDeps.akkaJackson,
    TestDeps.akkaStreamTyped,
    TestDeps.logback,
    TestDeps.scalaTest)

  val docs = Seq(
    TestDeps.akkaPersistenceTyped,
    TestDeps.akkaShardingTyped,
    TestDeps.akkaJackson,
    TestDeps.akkaTestkit,
    TestDeps.scalaTest)
}
