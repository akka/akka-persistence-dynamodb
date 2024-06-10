/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config = {
    val defaultConfig = ConfigFactory.load()

    ConfigFactory
      .parseString("""
      akka.loglevel = DEBUG
      akka.persistence.journal.plugin = "akka.persistence.dynamodb.journal"
      # FIXME akka.persistence.snapshot-store.plugin = "akka.persistence.dynamodb.snapshot"
      akka.persistence.dynamodb {
        query {
          refresh-interval = 1s
        }
        client {
          region = "us-west-2"
          credentials {
            access-key-id = "dummyKey"
            secret-access-key = "dummySecret"
          }
        }
      }
      akka.actor.testkit.typed.default-timeout = 10s
      """)
      .withFallback(defaultConfig)
  }

}
