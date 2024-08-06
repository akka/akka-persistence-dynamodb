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

  val ttlForDeletesConfig: Config = ConfigFactory.load(ConfigFactory.parseString("""
    //#use-time-to-live-for-deletes
    akka.persistence.dynamodb.time-to-live {
      event-sourced-entities {
        "some-entity-type" {
          use-time-to-live-for-deletes = 7 days
        }
      }
    }
    //#use-time-to-live-for-deletes
    """))

  val ttlConfig: Config = ConfigFactory.load(ConfigFactory.parseString("""
    //#time-to-live
    akka.persistence.dynamodb.time-to-live {
      event-sourced-entities {
        "entity-type-*" {
          event-time-to-live = 3 days
          snapshot-time-to-live = 5 days
        }
      }
    }
    //#time-to-live
    """))
}

class TimeToLiveSettingsDocExample extends AnyWordSpec with Matchers {
  import TimeToLiveSettingsDocExample._

  def dynamoDBSettings(config: Config): DynamoDBSettings =
    DynamoDBSettings(config.getConfig("akka.persistence.dynamodb"))

  "Journal Time to Live (TTL) docs" should {

    "have example of setting use-time-to-live-for-deletes" in {
      val settings = dynamoDBSettings(ttlForDeletesConfig)

      val ttlSettings = settings.timeToLiveSettings.eventSourcedEntities.get("some-entity-type")
      ttlSettings.checkExpiry shouldBe true
      ttlSettings.useTimeToLiveForDeletes shouldBe Some(7.days)
      ttlSettings.eventTimeToLive shouldBe None
      ttlSettings.snapshotTimeToLive shouldBe None

      val defaultTtlSettings = settings.timeToLiveSettings.eventSourcedEntities.get("other-entity-type")
      defaultTtlSettings.checkExpiry shouldBe true
      defaultTtlSettings.useTimeToLiveForDeletes shouldBe None
      defaultTtlSettings.eventTimeToLive shouldBe None
      defaultTtlSettings.snapshotTimeToLive shouldBe None
    }

    "have example of setting event-time-to-live and snapshot-time-to-live" in {
      val settings = dynamoDBSettings(ttlConfig)

      val ttlSettings1 = settings.timeToLiveSettings.eventSourcedEntities.get("entity-type-1")
      ttlSettings1.checkExpiry shouldBe true
      ttlSettings1.useTimeToLiveForDeletes shouldBe None
      ttlSettings1.eventTimeToLive shouldBe Some(3.days)
      ttlSettings1.snapshotTimeToLive shouldBe Some(5.days)

      val ttlSettings2 = settings.timeToLiveSettings.eventSourcedEntities.get("entity-type-2")
      ttlSettings2.checkExpiry shouldBe true
      ttlSettings2.useTimeToLiveForDeletes shouldBe None
      ttlSettings2.eventTimeToLive shouldBe Some(3.days)
      ttlSettings2.snapshotTimeToLive shouldBe Some(5.days)

      val defaultTtlSettings = settings.timeToLiveSettings.eventSourcedEntities.get("other-entity-type")
      defaultTtlSettings.checkExpiry shouldBe true
      defaultTtlSettings.useTimeToLiveForDeletes shouldBe None
      defaultTtlSettings.eventTimeToLive shouldBe None
      defaultTtlSettings.snapshotTimeToLive shouldBe None
    }

  }
}
