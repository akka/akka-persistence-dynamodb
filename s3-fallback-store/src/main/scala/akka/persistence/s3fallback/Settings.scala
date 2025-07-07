/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.s3fallback

import com.typesafe.config.Config
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueType

import scala.concurrent.duration._
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.Try

import java.util.Objects

object S3FallbackSettings {
  def apply(config: Config): S3FallbackSettings = {
    val clientPath = config.getString("http-client-config")
    val eventsBucket = config.getString("events-bucket")
    val snapshotsBucket = config.getString("snapshots-bucket")
    val minioLocal = MinioLocalSettings(config.getConfig("minio-local"))
    val region = if (config.hasPath("region")) Some(config.getString("region")).filter(_.nonEmpty) else None
    val multipart = MultipartSettings(config.getValue("multipart"))
    val warming = WarmingSettings(config.getValue("connection-warming"))

    new S3FallbackSettings(clientPath, eventsBucket, snapshotsBucket, minioLocal, region, multipart, warming)
  }
}

final class S3FallbackSettings(
    val httpClientPath: String,
    val eventsBucket: String,
    val snapshotsBucket: String,
    val minioLocal: MinioLocalSettings,
    val region: Option[String],
    val multipart: MultipartSettings,
    val warming: WarmingSettings) {
  require(httpClientPath.nonEmpty, "must include an HTTP client config path")
  require(
    eventsBucket.nonEmpty || snapshotsBucket.nonEmpty,
    "must include at least one of events bucket or snapshots bucket")

  override def toString: String =
    s"S3FallbackSettings(httpClientPath=$httpClientPath, eventsBucket=$eventsBucket, snapshotsBucket=$snapshotsBucket, " +
    s"minioLocal=$minioLocal" +
    (if (multipart != MultipartSettings.Empty) s", multipart=$multipart" else "") +
    (if (warming != WarmingSettings.Empty) s", warming=$warming" else "") +
    ")"
}

object MultipartSettings {
  def apply(configValue: ConfigValue): MultipartSettings =
    configValue.valueType match {
      case ConfigValueType.OBJECT =>
        val config = configValue.atKey("multipart").getConfig("multipart")

        if (config.hasPath("threshold") && config.hasPath("partition")) {
          val threshold = Try[Long](config.getBytes("threshold")).getOrElse(Long.MaxValue)
          val partition = config.getBytes("partition")

          new MultipartSettings(threshold, partition)
        } else Empty

      case _ =>
        Empty
    }

  val Empty = new MultipartSettings(Long.MaxValue, 0)
}

final class MultipartSettings(val threshold: Long, val partition: Long) {
  require(threshold > 5 * 1024 * 1024, "threshold must be at least 5 MiB")
  require(
    threshold == Long.MaxValue || partition >= 5 * 1024 * 1024,
    "partition must be at least 5 MiB if multipart is enabled")

  def enabled: Boolean = threshold < Long.MaxValue

  override def toString: String = s"MultipartSettings(threshold=${threshold}B, partition=${partition}B)"
  override def equals(other: Any): Boolean =
    other match {
      case o: MultipartSettings =>
        (this eq o) ||
          (!enabled && !o.enabled) || // Treat any disabled as equal
          ((threshold == o.threshold) && (partition == o.partition))

      case _ => false
    }

  override def hashCode: Int =
    if (!enabled) 0
    else Objects.hash(threshold, partition)
}

object WarmingSettings {
  def apply(configValue: ConfigValue): WarmingSettings =
    configValue.valueType match {
      case ConfigValueType.OBJECT =>
        val config = configValue.atKey("warming").getConfig("warming")

        if (config.hasPath("target") && config.hasPath("period")) {
          val target = config.getInt("target")
          val period = config.getDuration("period").toScala

          new WarmingSettings(target, period)
        } else Empty
      case _ => Empty
    }

  val Empty = new WarmingSettings(0, Duration.Zero)
}

final class WarmingSettings(val target: Int, val period: FiniteDuration) {
  require(target >= 0, "target must be non-negative")

  def enabled: Boolean = target > 0

  override def toString: String = s"WarmingSettings(target = $target, period = $period)"
  override def equals(other: Any): Boolean =
    other match {
      case o: WarmingSettings =>
        (this eq o) ||
          (!enabled && !o.enabled) || // treat any disabled as equal
          ((target == o.target) && (period == o.period))

      case _ => false
    }

  override def hashCode: Int =
    if (!enabled) 0
    else Objects.hash(target, period)
}

object MinioLocalSettings {
  def apply(config: Config): MinioLocalSettings = {
    val enabled = config.getBoolean("enabled")
    val host = config.getString("host")
    val port = config.getInt("port")
    val accessKey = config.getString("access-key")
    val secretKey = config.getString("secret-key")

    new MinioLocalSettings(enabled, host, port, accessKey, secretKey)
  }
}

final class MinioLocalSettings(
    val enabled: Boolean,
    val host: String,
    val port: Int,
    val accessKey: String,
    val secretKey: String) {
  override def toString: String =
    if (enabled) s"MinioLocalSettings(host=$host, port=$port)" else "disabled"

  override def equals(other: Any): Boolean =
    other match {
      case o: MinioLocalSettings =>
        (this eq o) ||
          (!enabled && !o.enabled) || // Treat any disabled as equal
          (enabled == o.enabled) &&
          (host == o.host) &&
          (port == o.port) &&
          (accessKey == o.accessKey) &&
          (secretKey == o.secretKey)

      case _ => false
    }

  override def hashCode: Int =
    if (!enabled) 0
    else Objects.hash(host, port, accessKey, secretKey)
}

final class HttpSettings(
    val maxConcurrency: Int,
    val maxPendingConnectionAcquires: Int,
    val readTimeout: FiniteDuration,
    val writeTimeout: FiniteDuration,
    val connectionTimeout: FiniteDuration,
    val connectionAcquisitionTimeout: FiniteDuration,
    val connectionTimeToLive: FiniteDuration,
    val useIdleConnectionReaper: Boolean,
    val connectionMaxIdleTime: FiniteDuration,
    val tlsNegotiationTimeout: FiniteDuration,
    val tcpKeepAlive: Boolean) {

  override def toString: String =
    s"HttpSettings(" +
    s"maxConcurrency=$maxConcurrency, " +
    s"maxPendingConnectionAcquires=$maxPendingConnectionAcquires, " +
    s"readTimeout=${readTimeout.toCoarsest}, " +
    s"writeTimeout=${writeTimeout.toCoarsest}, " +
    s"connectionTimeout=${connectionTimeout.toCoarsest}, " +
    s"connectionAcquisitionTimeout=${connectionAcquisitionTimeout.toCoarsest}, " +
    s"connectionTimeToLive=${connectionTimeToLive.toCoarsest}, " +
    s"useIdleConnectionReaper=$useIdleConnectionReaper, " +
    s"connectionMaxIdleTime=${connectionMaxIdleTime.toCoarsest}, " +
    s"tlsNegotiationTimeout=${tlsNegotiationTimeout.toCoarsest}, " +
    s"tcpKeepAlive=$tcpKeepAlive)"
}

object HttpSettings {
  def apply(config: Config): HttpSettings = {
    new HttpSettings(
      maxConcurrency = config.getInt("max-concurrency"),
      maxPendingConnectionAcquires = config.getInt("max-pending-connection-acquires"),
      readTimeout = config.getDuration("read-timeout").toScala,
      writeTimeout = config.getDuration("write-timeout").toScala,
      connectionTimeout = config.getDuration("connection-timeout").toScala,
      connectionAcquisitionTimeout = config.getDuration("connection-acquisition-timeout").toScala,
      connectionTimeToLive = config.getDuration("connection-time-to-live").toScala,
      useIdleConnectionReaper = config.getBoolean("use-idle-connection-reaper"),
      connectionMaxIdleTime = config.getDuration("connection-max-idle-time").toScala,
      tlsNegotiationTimeout = config.getDuration("tls-negotiation-timeout").toScala,
      tcpKeepAlive = config.getBoolean("tcp-keep-alive"))
  }
}
