/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.util

import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

import software.amazon.awssdk.metrics.MetricCollection
import software.amazon.awssdk.metrics.MetricPublisher

import scala.jdk.CollectionConverters.ListHasAsScala

import java.util.concurrent.ConcurrentHashMap

/**
 * Service Provider Interface for injecting AWS SDK MetricPublisher into the underlying DynamoDB client (see
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/metrics-list.html).
 *
 * Implementations must include a single constructor with one argument: an `akka.actor.ClassicActorSystemProvider`. To
 * setup your implementation, add a setting to your 'application.conf':
 *
 * {{{
 * akka.persistence.dynamodb.client.metrics-providers += com.myexample.MyAWSMetricsProvider
 * }}}
 */
@ApiMayChange
trait SDKClientMetricsProvider {

  /**
   * Given an overall config path for Akka Persistence DynamoDB (e.g. 'akka.persistence.dynamodb') returns an instance
   * of an AWS SDK MetricPublisher which publishes SDK client metrics to the location of this implementation's choosing.
   */
  def metricsProviderFor(configLocation: String): MetricPublisher
}

/** INTERNAL API */
@InternalApi
private[dynamodb] object SDKClientMetricsResolver {
  def resolve(system: ClassicActorSystemProvider): Option[SDKClientMetricsProvider] = {
    val providersPath = "akka.persistence.dynamodb.client.metrics-providers"
    val config = system.classicSystem.settings.config
    if (!config.hasPath(providersPath)) {
      None
    } else {
      val fqcns = config.getStringList(providersPath)

      fqcns.size match {
        case 0 => None
        case 1 => Some(createProvider(system, fqcns.get(0)))
        case _ =>
          val providers = fqcns.asScala.toSeq.map(fqcn => createProvider(system, fqcn))
          Some(new EnsembleSDKClientMetricsProvider(providers))
      }
    }
  }

  def createProvider(system: ClassicActorSystemProvider, fqcn: String): SDKClientMetricsProvider = {
    system.classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .dynamicAccess
      .createInstanceFor[SDKClientMetricsProvider](fqcn, List(classOf[ClassicActorSystemProvider] -> system))
      .get
  }

  // This technically does not follow the construction convention that would allow it
  // to be reflectively constructed, but we don't reflectively construct it
  private class EnsembleSDKClientMetricsProvider(providers: Seq[SDKClientMetricsProvider])
      extends SDKClientMetricsProvider {
    def metricsProviderFor(configLocation: String): MetricPublisher =
      instances.computeIfAbsent(
        configLocation,
        path =>
          new MetricPublisher {
            private val publishers = providers.map(_.metricsProviderFor(configLocation))

            def publish(metricCollection: MetricCollection): Unit = {
              publishers.foreach(_.publish(metricCollection))
            }

            def close(): Unit = {
              publishers.foreach(_.close())
            }
          })

    private val instances = new ConcurrentHashMap[String, MetricPublisher]()
  }
}
