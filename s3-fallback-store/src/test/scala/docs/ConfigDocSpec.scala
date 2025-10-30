/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package docs

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConfigDocSpec extends AnyWordSpec with Matchers {
  val s =
    """
  // #enable-for-events
  akka.persistence.dynamodb.journal {
    fallback-store = {
      plugin = "akka.persistence.s3-fallback-store"

      # Use the fallback store for write batches estimated to exceed 350 KiB
      threshold = 350 KiB
  
      # When reading or writing events from fallback for an entity, this many
      # requests to S3 (per entity) can be in-flight
      batch-size = 8
    }
  }
  // #enable-for-events
  
  // #enable-for-snapshots
  akka.persistence.dynamodb.snapshot {
    fallback-store = {
      plugin = "akka.persistence.s3-fallback-store"

      # Use the fallback store for writes exceeding 250 KiB
      threshold = 250 KiB
    }
  }
  // #enable-for-snapshots
    """

  "Doc config" should {
    "have parseable config" in {
      val config = ConfigFactory.parseString(s)

      config.getBytes("akka.persistence.dynamodb.journal.fallback-store.threshold") shouldBe (350 * 1024)
    }
  }
}
