/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.actor.typed.ActorSystem
import com.typesafe.config.Config

object DynamoDBSettings {

  /**
   * Scala API: Load configuration from `akka.persistence.dynamodb`.
   */
  def apply(system: ActorSystem[_]): DynamoDBSettings =
    apply(system.settings.config.getConfig("akka.persistence.dynamodb"))

  /**
   * Java API: Load configuration from `akka.persistence.dynamodb`.
   */
  def create(system: ActorSystem[_]): DynamoDBSettings =
    apply(system)

  /**
   * Scala API: From custom configuration corresponding to `akka.persistence.dynamodb`.
   */
  def apply(config: Config): DynamoDBSettings = {
    val journalTable: String = config.getString("journal.table")

    val snapshotTable: String = config.getString("snapshot.table")

    val querySettings = new QuerySettings(config.getConfig("query"))

    new DynamoDBSettings(journalTable, snapshotTable, querySettings)
  }

  /**
   * Java API: From custom configuration corresponding to `akka.persistence.dynamodb`.
   */
  def create(config: Config): DynamoDBSettings =
    apply(config)

}

final class DynamoDBSettings private (
    val journalTable: String,
    val snapshotTable: String,
    val querySettings: QuerySettings) {

  val journalBySliceGsi: String = journalTable + "_slice_idx"

  override def toString =
    s"DynamoDBSettings($journalTable, $querySettings)"
}

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
