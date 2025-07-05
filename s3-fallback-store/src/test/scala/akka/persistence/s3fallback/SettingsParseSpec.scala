/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.s3fallback

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava

class SettingsParseSpec extends AnyWordSpec with Matchers {
  "S3 Fallback Settings" should {

    val referenceWithSnapshotsBucket =
      ConfigValueFactory
        .fromMap(Map("snapshots-bucket" -> "foo").asJava)
        .atPath("akka.persistence.s3-fallback-store")
        .withFallback(ConfigFactory.load(ConfigFactory.defaultReference))

    "have a parseable reference.conf" in {
      val semanticException = the[IllegalArgumentException] thrownBy {
        val config =
          ConfigFactory.load(ConfigFactory.defaultReference).getConfig("akka.persistence.s3-fallback-store")

        S3FallbackSettings(config)
      }

      semanticException.getMessage should include("must include at least one of events bucket or snapshots bucket")
    }

    "parse multipart settings" in {
      val multipartConfig =
        ConfigValueFactory.fromMap(Map("threshold" -> 6 * 1024 * 1024, "partition" -> 5 * 1024 * 1024).asJava)

      val overrideConfig = ConfigValueFactory.fromMap(Map("multipart" -> multipartConfig).asJava)

      val config = overrideConfig
        .atPath("akka.persistence.s3-fallback-store")
        .withFallback(referenceWithSnapshotsBucket)
        .getConfig("akka.persistence.s3-fallback-store")

      val settings = S3FallbackSettings(config)

      settings.multipart.enabled shouldBe true
      settings.multipart.threshold shouldBe (6 * 1024 * 1024)
      settings.multipart.partition shouldBe (5 * 1024 * 1024)
    }

    "parse connection-warming settings" in {
      val warmingConfig =
        ConfigValueFactory
          .fromMap(Map("target" -> (2: Integer), "period" -> "3s").asJava)
          .atPath("akka.persistence.s3-fallback-store.connection-warming")

      val config =
        warmingConfig.withFallback(referenceWithSnapshotsBucket).getConfig("akka.persistence.s3-fallback-store")

      val settings = S3FallbackSettings(config)

      settings.warming.enabled shouldBe true
      settings.warming.target shouldBe 2
      settings.warming.period.toMillis shouldBe 3000
    }
  }
}
