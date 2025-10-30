/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.s3fallback

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.annotation.InternalApi
import akka.persistence.dynamodb.util.AWSClientMetricsResolver
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

import java.net.URI
import java.util.concurrent.ConcurrentHashMap

/** INTERNAL API */
@InternalApi
object S3ClientProvider extends ExtensionId[S3ClientProvider] {
  def createExtension(system: ActorSystem[_]): S3ClientProvider = new S3ClientProvider(system)

  def get(system: ActorSystem[_]): S3ClientProvider = apply(system)

  private[s3fallback] final case class S3ClientSpec(
      region: Option[String],
      minioLocalSettings: MinioLocalSettings,
      maxConcurrency: Int,
      maxPendingConnectionAcquires: Int,
      readTimeout: FiniteDuration,
      writeTimeout: FiniteDuration,
      connectionTimeout: FiniteDuration,
      connectionAcquisitionTimeout: FiniteDuration,
      connectionTimeToLive: FiniteDuration,
      useIdleConnectionReaper: Boolean,
      connectionMaxIdleTime: FiniteDuration,
      tlsNegotiationTimeout: FiniteDuration,
      tcpKeepAlive: Boolean,
      multipart: MultipartSettings)
}

/** INTERNAL API */
@InternalApi
final class S3ClientProvider(system: ActorSystem[_]) extends Extension {
  import S3ClientProvider.S3ClientSpec

  // only the minio local settings and HTTP client settings affect this
  private val clients = new ConcurrentHashMap[S3ClientSpec, S3AsyncClient]

  CoordinatedShutdown(system)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close S3 clients") { () =>
      // FIXME: as with DDB clients, should this be run on a blocking dispatcher?
      clients.asScala.values.foreach(_.close())
      Future.successful(Done)
    }

  def clientFor(settings: S3FallbackSettings, configLocation: String): S3AsyncClient = {
    val clientSettingsConfig = system.settings.config.getConfig(settings.httpClientPath)
    val http = HttpSettings(clientSettingsConfig)

    val spec = S3ClientSpec(
      settings.region,
      settings.minioLocal,
      http.maxConcurrency,
      http.maxPendingConnectionAcquires,
      http.readTimeout,
      http.writeTimeout,
      http.connectionTimeout,
      http.connectionAcquisitionTimeout,
      http.connectionTimeToLive,
      http.useIdleConnectionReaper,
      http.connectionMaxIdleTime,
      http.tlsNegotiationTimeout,
      http.tcpKeepAlive,
      settings.multipart)

    clientFor(spec, configLocation)
  }

  def clientFor(spec: S3ClientSpec, configLocation: String): S3AsyncClient =
    clients.computeIfAbsent(spec, settings => createClient(settings, configLocation))

  private def createClient(spec: S3ClientSpec, configLocation: String): S3AsyncClient = {
    // At least for now, use the same http client settings as DDB
    val httpClientBuilder = NettyNioAsyncHttpClient.builder
      .maxConcurrency(spec.maxConcurrency)
      .maxPendingConnectionAcquires(spec.maxPendingConnectionAcquires)
      .readTimeout(spec.readTimeout.toJava)
      .writeTimeout(spec.writeTimeout.toJava)
      .connectionTimeout(spec.connectionTimeout.toJava)
      .connectionAcquisitionTimeout(spec.connectionAcquisitionTimeout.toJava)
      .connectionTimeToLive(spec.connectionTimeToLive.toJava)
      .useIdleConnectionReaper(spec.useIdleConnectionReaper)
      .connectionMaxIdleTime(spec.connectionMaxIdleTime.toJava)
      .tlsNegotiationTimeout(spec.tlsNegotiationTimeout.toJava)
      .tcpKeepAlive(spec.tcpKeepAlive)

    var builder = S3AsyncClient.builder

    builder = builder
      .httpClientBuilder(httpClientBuilder)

    if (spec.multipart.enabled) {
      builder
        .multipartConfiguration { mpBuilder =>
          mpBuilder
            .thresholdInBytes(spec.multipart.threshold)
            .minimumPartSizeInBytes(spec.multipart.partition)
        }
        .multipartEnabled(true)
    }

    // otherwise default region lookup
    spec.region.foreach { region =>
      builder = builder.region(Region.of(region))
    }

    if (spec.minioLocalSettings.enabled) {
      // Use Minio
      val localSettings = spec.minioLocalSettings

      builder = builder
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create(s"http://${localSettings.host}:${localSettings.port}"))
        .credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create(localSettings.accessKey, localSettings.secretKey)))
        .forcePathStyle(true)
    }

    AWSClientMetricsResolver
      .resolve(system, configLocation + ".metrics-providers")
      .foreach { mp =>
        builder.overrideConfiguration { ocBuilder =>
          ocBuilder.addMetricPublisher(mp.metricPublisherFor(configLocation))
        }
      }

    builder.build()
  }
}
