package docs

// #cloudwatch-default
import akka.actor.ClassicActorSystemProvider
import akka.persistence.dynamodb.util.SDKClientMetricsProvider
import software.amazon.awssdk.metrics.MetricPublisher
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher

class CloudWatchWithDefaultConfigurationMetricsProvider(system: ClassicActorSystemProvider)
    extends SDKClientMetricsProvider {
  def metricsPublisherFor(configLocation: String): MetricPublisher = {
    // These are just the defaults... a more elaborate configuration using its builder is possible
    CloudWatchMetricPublisher.create()
  }
}
// #cloudwatch-default
