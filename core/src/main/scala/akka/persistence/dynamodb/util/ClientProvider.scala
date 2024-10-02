/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.util

import java.net.URI
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.persistence.dynamodb.ClientSettings
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.CompressionConfiguration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.metrics.MetricPublisher

object ClientProvider extends ExtensionId[ClientProvider] {
  def createExtension(system: ActorSystem[_]): ClientProvider = new ClientProvider(system)

  // Java API
  def get(system: ActorSystem[_]): ClientProvider = apply(system)
}
class ClientProvider(system: ActorSystem[_]) extends Extension {
  private val clients = new ConcurrentHashMap[String, DynamoDbAsyncClient]
  private val clientSettings = new ConcurrentHashMap[String, ClientSettings]
  private val metricsProvider = AWSClientMetricsResolver.resolve(system)

  CoordinatedShutdown(system)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close DynamoDB clients") { () =>
      // FIXME is this blocking, and should be run on blocking dispatcher?
      clients.asScala.values.foreach(_.close())
      Future.successful(Done)
    }

  def clientFor(configLocation: String): DynamoDbAsyncClient = {
    clients.computeIfAbsent(
      configLocation,
      configLocation => {
        val settings = clientSettingsFor(configLocation)
        createClient(settings, metricsProvider.map(_.metricPublisherFor(configLocation)))
      })
  }

  def clientSettingsFor(configLocation: String): ClientSettings = {
    clientSettings.get(configLocation) match {
      case null =>
        val settings = ClientSettings(system.settings.config.getConfig(configLocation))
        // it's just a cache so no need for guarding concurrent updates
        clientSettings.put(configLocation, settings)
        settings
      case settings => settings
    }
  }

  private def createClient(settings: ClientSettings, metricsPublisher: Option[MetricPublisher]): DynamoDbAsyncClient = {
    val httpClientBuilder = NettyNioAsyncHttpClient.builder
      .maxConcurrency(settings.http.maxConcurrency)
      .maxPendingConnectionAcquires(settings.http.maxPendingConnectionAcquires)
      .readTimeout(settings.http.readTimeout.toJava)
      .writeTimeout(settings.http.writeTimeout.toJava)
      .connectionTimeout(settings.http.connectionTimeout.toJava)
      .connectionAcquisitionTimeout(settings.http.connectionAcquisitionTimeout.toJava)
      .connectionTimeToLive(settings.http.connectionTimeToLive.toJava)
      .useIdleConnectionReaper(settings.http.useIdleConnectionReaper)
      .connectionMaxIdleTime(settings.http.connectionMaxIdleTime.toJava)
      .tlsNegotiationTimeout(settings.http.tlsNegotiationTimeout.toJava)
      .tcpKeepAlive(settings.http.tcpKeepAlive)

    val overrideConfiguration = {
      val retryPolicy = settings.retry.fold(RetryPolicy.none()) { retrySettings =>
        val builder = RetryPolicy.builder(retrySettings.mode)
        retrySettings.numRetries.foreach(numRetries => builder.numRetries(numRetries))
        builder.build()
      }

      val compressionConfiguration = CompressionConfiguration.builder
        .requestCompressionEnabled(settings.compression.enabled)
        .minimumCompressionThresholdInBytes(settings.compression.thresholdBytes)
        .build()

      var overrideConfigurationBuilder = ClientOverrideConfiguration.builder
        .apiCallTimeout(settings.callTimeout.toJava)
        .retryPolicy(retryPolicy)
        .compressionConfiguration(compressionConfiguration)

      settings.callAttemptTimeout.foreach { timeout =>
        overrideConfigurationBuilder = overrideConfigurationBuilder.apiCallAttemptTimeout(timeout.toJava)
      }

      metricsPublisher.foreach { mp => overrideConfigurationBuilder.addMetricPublisher(mp) }

      overrideConfigurationBuilder.build()
    }

    var clientBuilder = DynamoDbAsyncClient.builder
      .httpClientBuilder(httpClientBuilder)
      .overrideConfiguration(overrideConfiguration)

    // otherwise default region lookup
    settings.region.foreach { region =>
      clientBuilder = clientBuilder.region(Region.of(region))
    }

    // override endpoint, region, and credentials when local enabled
    settings.local.foreach { localSettings =>
      clientBuilder = clientBuilder
        .endpointOverride(URI.create(s"http://${localSettings.host}:${localSettings.port}"))
        .region(Region.US_WEST_2)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummyKey", "dummySecret")))
    }

    clientBuilder.build()
  }

}
