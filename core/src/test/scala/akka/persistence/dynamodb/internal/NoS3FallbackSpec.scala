/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.util.ClientProvider
import com.typesafe.config.ConfigFactory
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Failure

import java.time.Clock
import java.util.UUID

// This project doesn't test the real S3 fallback since it doesn't have the S3 client
// on the classpath...
class NoS3FallbackSpec extends AnyWordSpec with TestSuite with Matchers {
  val clock = Clock.systemUTC // Times don't actually matter in this test

  "NoS3Fallback" should {
    "be returned from S3Fallback.apply if S3 fallback is enabled but client isn't on classpath" in {
      val config =
        ConfigFactory
          .parseString("""
              akka.persistence.dynamodb.fallback-to-s3 {
                enabled = true
                events-bucket = "events"
                snapshots-bucket = "snapshot-bucket"
              }
            """)
          .withFallback(ConfigFactory.defaultReference)
          .resolve

      val system = ActorSystem[Nothing](Behaviors.empty[Nothing], "test", config)
      val settings = DynamoDBSettings(system)
      val clientSettings = ClientProvider(system).clientSettingsFor("akka.persistence.dynamodb.client")

      S3Fallback(settings, clientSettings, system) should contain(NoS3Fallback)
    }

    "not be returned if S3 fallback is not enabled" in {
      val system = ActorSystem(Behaviors.empty, "test")
      val settings = DynamoDBSettings(system)
      val clientSettings = ClientProvider(system).clientSettingsFor("akka.persistence.dynamodb.client")

      S3Fallback(settings, clientSettings, system) should be(empty)
    }

    "fail to load event" in {
      val result = NoS3Fallback.loadEvent("pid", 42, "somebucket", true)
      result.value shouldNot be(empty)
      result.value.get shouldBe a[Failure[_]]
      result.value.get.failed.get.getMessage shouldBe "S3 Client not on classpath"

      val result2 = NoS3Fallback.loadEvent("pid", 42, "somebucket", false)
      result2.value shouldNot be(empty)
      result2.value.get shouldBe a[Failure[_]]
      result2.value.get.failed.get.getMessage shouldBe "S3 Client not on classpath"
    }

    "fail to save event" in {
      val item =
        SerializedJournalItem(
          persistenceId = "pid",
          seqNr = 42,
          writeTimestamp = clock.instant(),
          readTimestamp = clock.instant(),
          payload = Some(Array(64.toByte)),
          serId = 1,
          serManifest = "NotReallyASerializedItem",
          writerUuid = UUID.randomUUID().toString,
          tags = Set.empty,
          metadata = None)

      val result = NoS3Fallback.saveEvent(item)
      result.value shouldNot be(empty)
      result.value.get shouldBe a[Failure[_]]
      result.value.get.failed.get.getMessage shouldBe "S3 Client not on classpath"
    }

    "fail to load snapshot" in {
      val result = NoS3Fallback.loadSnapshot("pid", "somebucket")
      result.value shouldNot be(empty)
      result.value.get shouldBe a[Failure[_]]
      result.value.get.failed.get.getMessage shouldBe "S3 Client not on classpath"
    }

    "fail to save snapshot" in {
      val item =
        SerializedSnapshotItem(
          persistenceId = "pid",
          seqNr = 42,
          writeTimestamp = clock.instant(),
          eventTimestamp = clock.instant(),
          payload = Array(64.toByte),
          serId = 1,
          serManifest = "NotReallyASerializedItem",
          tags = Set.empty,
          metadata = None)

      val result = NoS3Fallback.saveSnapshot(item)
      result.value shouldNot be(empty)
      result.value.get shouldBe a[Failure[_]]
      result.value.get.failed.get.getMessage shouldBe "S3 Client not on classpath"
    }
  }
}
