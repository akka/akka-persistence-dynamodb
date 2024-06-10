/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.actor.typed.ActorSystem
import akka.annotation.InternalStableApi
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

    val journalPublishEvents: Boolean = config.getBoolean("journal.publish-events")

    val snapshotTable: String = config.getString("snapshot.table")

    val querySettings = new QuerySettings(config.getConfig("query"))

    new DynamoDBSettings(journalTable, journalPublishEvents, snapshotTable, querySettings)
  }

  /**
   * Java API: From custom configuration corresponding to `akka.persistence.dynamodb`.
   */
  def create(config: Config): DynamoDBSettings =
    apply(config)

}

final class DynamoDBSettings private (
    val journalTable: String,
    val journalPublishEvents: Boolean,
    val snapshotTable: String,
    val querySettings: QuerySettings) {

  val journalBySliceGsi: String = journalTable + "_slice_idx"

}

final class QuerySettings(config: Config) {
  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval").toScala
  val behindCurrentTime: FiniteDuration = config.getDuration("behind-current-time").toScala
  val backtrackingEnabled: Boolean = config.getBoolean("backtracking.enabled")
  val backtrackingWindow: FiniteDuration = config.getDuration("backtracking.window").toScala
  val backtrackingBehindCurrentTime: FiniteDuration = config.getDuration("backtracking.behind-current-time").toScala
  val bufferSize: Int = config.getInt("buffer-size")
  val deduplicateCapacity: Int = config.getInt("deduplicate-capacity")
}

object ClientSettings {
  final class Credentials(val accessKeyId: String, val secretAccessKey: String)

  object Credentials {
    def get(clientConfig: Config): Option[Credentials] = {
      val config = clientConfig.getConfig("credentials")
      (optString(config, "access-key-id"), optString(config, "secret-access-key")) match {
        case (Some(accessKeyId), Some(secretAccessKey)) => Some(new Credentials(accessKeyId, secretAccessKey))
        case _                                          => None
      }
    }
  }

  def apply(config: Config): ClientSettings =
    new ClientSettings(
      host = config.getString("host"),
      port = config.getInt("port"),
      region = optString(config, "region"),
      credentials = Credentials.get(config))

  private def optString(config: Config, path: String): Option[String] = {
    if (config.hasPath(path)) {
      val value = config.getString(path)
      if (value.nonEmpty) Some(value) else None
    } else None
  }
}

final class ClientSettings(
    val host: String,
    val port: Int,
    val region: Option[String],
    val credentials: Option[ClientSettings.Credentials]) {
  override def toString: String =
    s"ClientSettings($host, $port)"
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class PublishEventsDynamicSettings(config: Config) {
  val throughputThreshold: Int = config.getInt("throughput-threshold")
  val throughputCollectInterval: FiniteDuration = config.getDuration("throughput-collect-interval").toScala
}
