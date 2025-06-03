/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.dynamodb.DynamoDBSettings
import com.typesafe.config.ConfigFactory

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import akka.persistence.dynamodb.util.ClientProvider

class S3FallbackDetectionSpec extends AnyWordSpec with TestSuite with Matchers {
  "S3Fallback" should {
    "not be returned if S3 fallback is not enabled" in {
      val config =
        ConfigFactory
          .parseString("""
          akka.persistence.dynamodb.fallback-to-s3.enabled = false
          """)
          .withFallback(ConfigFactory.defaultReference)
          .resolve

      val system = ActorSystem[Nothing](Behaviors.empty, "test", config)
      val settings = DynamoDBSettings(system)
      val clientSettings = ClientProvider(system).clientSettingsFor("akka.persistence.dynamodb.client")

      S3Fallback(settings, clientSettings, system) should be(empty)
    }

    "be returned if S3 fallback is enabled and client is on classpath" in {
      val config =
        ConfigFactory.parseResources("application-test.conf").withFallback(ConfigFactory.defaultReference).resolve
      val system = ActorSystem[Nothing](Behaviors.empty, "test", config)
      val settings = DynamoDBSettings(system)
      val clientSettings = ClientProvider(system).clientSettingsFor("akka.persistence.dynamodb.client")

      val result = S3Fallback(settings, clientSettings, system)
      result shouldNot be(empty)
      result.get shouldBe a[RealS3Fallback]
    }
  }
}
