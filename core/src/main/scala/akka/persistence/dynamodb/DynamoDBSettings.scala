/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.jdk.DurationConverters._
import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalStableApi
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalStableApi
object DynamoDBSettings {

  def apply(config: Config): DynamoDBSettings = {
    val journalTable: String = config.getString("journal.table")

    val querySettings = new QuerySettings(config.getConfig("query"))

    new DynamoDBSettings(journalTable, querySettings)
  }

}

/**
 * INTERNAL API
 */
@InternalStableApi
final class DynamoDBSettings private (val journalTable: String, val querySettings: QuerySettings) {

  val journalBySliceGsi: String = journalTable + "_slice_idx"

  override def toString =
    s"DynamoDBSettings($journalTable, $querySettings)"
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class QuerySettings(config: Config) {
  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval").toScala
  val behindCurrentTime: FiniteDuration = config.getDuration("behind-current-time").toScala
  val backtrackingEnabled: Boolean = config.getBoolean("backtracking.enabled")
  val backtrackingWindow: FiniteDuration = config.getDuration("backtracking.window").toScala
  val backtrackingBehindCurrentTime: FiniteDuration = config.getDuration("backtracking.behind-current-time").toScala
  val bufferSize: Int = config.getInt("buffer-size")
}

object ClientSettings {
  def apply(config: Config): ClientSettings =
    new ClientSettings(host = config.getString("host"), port = config.getInt("port"))
}

final class ClientSettings(val host: String, val port: Int) {
  override def toString: String =
    s"ClientSettings($host, $port)"
}
