/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.util

object TableSettings {

  /**
   * Default table settings for running with DynamoDB local.
   */
  val Local = new TableSettings(throughput = ThroughputSettings.Local)

  /**
   * Scala API: Create table settings for CreateTables utilities.
   *
   * @param throughput
   *   throughput settings for the table
   * @return
   *   table settings
   */
  def apply(throughput: ThroughputSettings): TableSettings =
    new TableSettings(throughput)

  /**
   * Java API: Create table settings for CreateTables utilities.
   *
   * @param throughput
   *   throughput settings for the table
   * @return
   *   table settings
   */
  def create(throughput: ThroughputSettings): TableSettings =
    new TableSettings(throughput)
}

/**
 * Table settings for CreateTables utilities.
 *
 * @param throughput
 *   throughput settings for the table
 */
final class TableSettings(val throughput: ThroughputSettings)

object IndexSettings {

  /**
   * Default index settings for running with DynamoDB local.
   */
  val Local = new IndexSettings(enabled = true, throughput = ThroughputSettings.Local)

  /**
   * Disabled index settings.
   */
  val Disabled = new IndexSettings(enabled = false, throughput = ThroughputSettings.Local)

  /**
   * Scala API: Create index settings for CreateTables utilities.
   *
   * @param throughput
   *   throughput settings for the index
   * @return
   *   index settings for an enabled index
   */
  def apply(throughput: ThroughputSettings): IndexSettings =
    new IndexSettings(enabled = true, throughput)

  /**
   * Java API: Create index settings for CreateTables utilities.
   *
   * @param throughput
   *   throughput settings for the index
   * @return
   *   index settings for an enabled index
   */
  def create(throughput: ThroughputSettings): IndexSettings =
    new IndexSettings(enabled = true, throughput)
}

/**
 * Index settings for CreateTables utilities.
 *
 * @param enabled
 *   whether the index is enabled
 * @param throughput
 *   throughput settings for the index
 */
final class IndexSettings(val enabled: Boolean, val throughput: ThroughputSettings)

object ThroughputSettings {

  /**
   * Default throughput settings for running with DynamoDB local.
   */
  val Local: ThroughputSettings = provisioned(readCapacityUnits = 5L, writeCapacityUnits = 5L)

  /**
   * Create provisioned throughput settings.
   *
   * @param readCapacityUnits
   *   the maximum number of strongly consistent reads consumed per second
   * @param writeCapacityUnits
   *   the maximum number of writes consumed per second
   * @return
   *   provisioned throughput settings
   */
  def provisioned(readCapacityUnits: Long, writeCapacityUnits: Long): ThroughputSettings =
    new ProvisionedThroughputSettings(readCapacityUnits, writeCapacityUnits)

  /**
   * Create on-demand throughput settings.
   *
   * @param maxReadRequestUnits
   *   the maximum number of read request units (for no maximum, set to -1)
   * @param maxWriteRequestUnits
   *   the maximum number of write request units (for no maximum, set to -1)
   * @return
   *   on-demand throughput settings
   */
  def onDemand(maxReadRequestUnits: Long, maxWriteRequestUnits: Long): ThroughputSettings =
    new OnDemandThroughputSettings(maxReadRequestUnits, maxWriteRequestUnits)
}

sealed trait ThroughputSettings

/**
 * Provisioned throughput settings.
 *
 * @param readCapacityUnits
 *   the maximum number of strongly consistent reads consumed per second
 * @param writeCapacityUnits
 *   the maximum number of writes consumed per second
 */
final class ProvisionedThroughputSettings(val readCapacityUnits: Long, val writeCapacityUnits: Long)
    extends ThroughputSettings

/**
 * On-demand throughput settings.
 *
 * @param maxReadRequestUnits
 *   the maximum number of read request units (for no maximum, set to -1)
 * @param maxWriteRequestUnits
 *   the maximum number of write request units (for no maximum, set to -1)
 */
final class OnDemandThroughputSettings(val maxReadRequestUnits: Long, val maxWriteRequestUnits: Long)
    extends ThroughputSettings
