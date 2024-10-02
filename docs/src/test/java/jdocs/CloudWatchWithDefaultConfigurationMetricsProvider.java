package jdocs;

// #cloudwatch-default
import akka.actor.ClassicActorSystemProvider;
import akka.persistence.dynamodb.util.AWSClientMetricsProvider;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher;

public class CloudWatchWithDefaultConfigurationMetricsProvider implements AWSClientMetricsProvider {
  public CloudWatchWithDefaultConfigurationMetricsProvider(ClassicActorSystemProvider system) {
  }

  @Override
  public MetricPublisher metricPublisherFor(String configLocation) {
    // These are just the defaults... a more elaborate configuration using its builder is possible
    return CloudWatchMetricPublisher.create();
  }
}
// #cloudwatch-default
