/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package docs

// #cloudwatch-default
import akka.actor.ClassicActorSystemProvider
import akka.persistence.dynamodb.util.AWSClientMetricsProvider
import software.amazon.awssdk.metrics.MetricPublisher
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher

class CloudWatchWithDefaultConfigurationMetricsProvider(system: ClassicActorSystemProvider)
    extends AWSClientMetricsProvider {
  def metricPublisherFor(configLocation: String): MetricPublisher = {
    // These are just the defaults... a more elaborate configuration using its builder is possible
    CloudWatchMetricPublisher.create()
  }
}
// #cloudwatch-default
