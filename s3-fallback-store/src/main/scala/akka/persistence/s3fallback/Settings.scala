/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.s3fallback

import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

import java.util.Objects

object S3FallbackSettings {
  def apply(config: Config): S3FallbackSettings = {
    val clientPath = config.getString("http-client-config")
    val eventsBucket = config.getString("events-bucket")
    val snapshotsBucket = config.getString("snapshots-bucket")
    val minioLocal = MinioLocalSettings(config.getConfig("minio-local"))
    val region = if (config.hasPath("region")) Some(config.getString("region")).filter(_.nonEmpty) else None

    new S3FallbackSettings(clientPath, eventsBucket, snapshotsBucket, minioLocal, region)
  }
}

final class S3FallbackSettings(
    val httpClientPath: String,
    val eventsBucket: String,
    val snapshotsBucket: String,
    val minioLocal: MinioLocalSettings,
    val region: Option[String]) {
  require(httpClientPath.nonEmpty, "must include an HTTP client config path")
  require(eventsBucket.nonEmpty, "if enabled, eventsBucket must not be empty")
  require(snapshotsBucket.nonEmpty, "if enabled, snapshotBucket must not be empty")

  override def toString: String =
    s"S3FallbackSettings(httpClientPath=$httpClientPath, eventsBucket=$eventsBucket, snapshotsBucket=$snapshotsBucket, minioLocal=$minioLocal)"
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
