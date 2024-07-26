/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.config

import scala.concurrent.duration._

import akka.persistence.dynamodb.DynamoDBSettings
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object TimeToLiveSettingsDocExample {

  val checkExpiryConfig: Config = ConfigFactory.load(ConfigFactory.parseString("""
    //#check-expiry
    akka.persistence.dynamodb.time-to-live {
     check-expiry = on
    }
    //#check-expiry
    """))

  val ttlForDeletesConfig: Config = ConfigFactory.load(ConfigFactory.parseString("""
    //#use-time-to-live-for-deletes
    akka.persistence.dynamodb.time-to-live {
      use-time-to-live-for-deletes = 7 days
    }
    //#use-time-to-live-for-deletes
    """))
}

class TimeToLiveSettingsDocExample extends AnyWordSpec with Matchers {
  import TimeToLiveSettingsDocExample._

  def dynamoDBSettings(config: Config): DynamoDBSettings =
    DynamoDBSettings(config.getConfig("akka.persistence.dynamodb"))

  "Journal Time to Live (TTL) docs" should {

    "have example of setting check-expiry" in {
      val settings = dynamoDBSettings(checkExpiryConfig)
      settings.timeToLiveSettings.checkExpiry shouldBe true
      settings.timeToLiveSettings.useTimeToLiveForDeletes shouldBe None
    }

    "have example of setting use-time-to-live-for-deletes" in {
      val settings = dynamoDBSettings(ttlForDeletesConfig)
      settings.timeToLiveSettings.checkExpiry shouldBe false
      settings.timeToLiveSettings.useTimeToLiveForDeletes shouldBe Some(7.days)
    }

  }
}
