/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.time.{ Duration => JDuration }

import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

object DynamoDBProjectionSettings {

  val DefaultConfigPath = "akka.projection.dynamodb"

  /**
   * Scala API: Load configuration from `akka.projection.dynamodb`.
   */
  def apply(system: ActorSystem[_]): DynamoDBProjectionSettings =
    apply(system.settings.config.getConfig(DefaultConfigPath))

  /**
   * Java API: Load configuration from `akka.projection.dynamodb`.
   */
  def create(system: ActorSystem[_]): DynamoDBProjectionSettings =
    apply(system)

  /**
   * Scala API: From custom configuration corresponding to `akka.projection.dynamodb`.
   */
  def apply(config: Config): DynamoDBProjectionSettings = {
    new DynamoDBProjectionSettings(
      timestampOffsetTable = config.getString("offset-store.timestamp-offset-table"),
      useClient = config.getString("use-client"),
      timeWindow = config.getDuration("offset-store.time-window"),
      keepNumberOfEntries = config.getInt("offset-store.keep-number-of-entries"),
      evictInterval = config.getDuration("offset-store.evict-interval"),
      warnAboutFilteredEventsInFlow = config.getBoolean("warn-about-filtered-events-in-flow"),
      offsetBatchSize = config.getInt("offset-store.offset-batch-size"))
  }

  /**
   * Java API: From custom configuration corresponding to `akka.projection.dynamodb`.
   */
  def create(config: Config): DynamoDBProjectionSettings =
    apply(config)

}

final class DynamoDBProjectionSettings private (
    val timestampOffsetTable: String,
    val useClient: String,
    val timeWindow: JDuration,
    val keepNumberOfEntries: Int,
    val evictInterval: JDuration,
    val warnAboutFilteredEventsInFlow: Boolean,
    val offsetBatchSize: Int) {

  def withTimestampOffsetTable(timestampOffsetTable: String): DynamoDBProjectionSettings =
    copy(timestampOffsetTable = timestampOffsetTable)

  def withUseClient(clientConfigPath: String): DynamoDBProjectionSettings =
    copy(useClient = clientConfigPath)

  def withTimeWindow(timeWindow: FiniteDuration): DynamoDBProjectionSettings =
    copy(timeWindow = timeWindow.asJava)

  def withTimeWindow(timeWindow: JDuration): DynamoDBProjectionSettings =
    copy(timeWindow = timeWindow)

  def withKeepNumberOfEntries(keepNumberOfEntries: Int): DynamoDBProjectionSettings =
    copy(keepNumberOfEntries = keepNumberOfEntries)

  def withEvictInterval(evictInterval: FiniteDuration): DynamoDBProjectionSettings =
    copy(evictInterval = evictInterval.asJava)

  def withEvictInterval(evictInterval: JDuration): DynamoDBProjectionSettings =
    copy(evictInterval = evictInterval)

  def withWarnAboutFilteredEventsInFlow(warnAboutFilteredEventsInFlow: Boolean): DynamoDBProjectionSettings =
    copy(warnAboutFilteredEventsInFlow = warnAboutFilteredEventsInFlow)

  def withOffsetBatchSize(offsetBatchSize: Int): DynamoDBProjectionSettings =
    copy(offsetBatchSize = offsetBatchSize)

  private def copy(
      timestampOffsetTable: String = timestampOffsetTable,
      useClient: String = useClient,
      timeWindow: JDuration = timeWindow,
      keepNumberOfEntries: Int = keepNumberOfEntries,
      evictInterval: JDuration = evictInterval,
      warnAboutFilteredEventsInFlow: Boolean = warnAboutFilteredEventsInFlow,
      offsetBatchSize: Int = offsetBatchSize) =
    new DynamoDBProjectionSettings(
      timestampOffsetTable,
      useClient,
      timeWindow,
      keepNumberOfEntries,
      evictInterval,
      warnAboutFilteredEventsInFlow,
      offsetBatchSize)

  override def toString =
    s"DynamoDBProjectionSettings($timestampOffsetTable, $useClient, $timeWindow, $keepNumberOfEntries, $evictInterval, $warnAboutFilteredEventsInFlow, $offsetBatchSize)"
}
