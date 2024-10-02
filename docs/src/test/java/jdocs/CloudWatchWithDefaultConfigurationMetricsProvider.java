package jdocs;

// #cloudwatch-default
import akka.actor.ClassicActorSystemProvider;
import akka.persistence.dynamodb.util.SDKClientMetricsProvider;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher;

public class CloudWatchWithDefaultConfigurationMetricsProvider implements SDKClientMetricsProvider {
  public CloudWatchWithDefaultConfigurationMetricsProvider(ClassicActorSystemProvider system) {
  }

  @Override
  public MetricPublisher metricsPublisherFor(String configLocation) {
    // These are just the defaults... a more elaborate configuration using its builder is possible
    return CloudWatchMetricPublisher.create();
  }
}
// #cloudwatch-default
