/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import akka.annotation.InternalStableApi
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalStableApi
object DynamoDBSettings {

  def apply(config: Config): DynamoDBSettings = {
    val journalTable: String = config.getString("journal.table")
    new DynamoDBSettings(journalTable)
  }

}

/**
 * INTERNAL API
 */
@InternalStableApi
final class DynamoDBSettings private (val journalTable: String) {

  override def toString =
    s"DynamoDBSettings($journalTable)"
}

object ClientSettings {
  def apply(config: Config): ClientSettings =
    new ClientSettings(host = config.getString("host"), port = config.getInt("port"))
}

final class ClientSettings(val host: String, val port: Int) {
  override def toString: String =
    s"ClientSettings($host, $port)"
}
