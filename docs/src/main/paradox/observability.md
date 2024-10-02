# Observability

This plugin supports injecting an [AWS `MetricPublisher`](https://github.com/aws/aws-sdk-java-v2/blob/master/docs/design/core/metrics/Design.md) into the underlying DynamoDB SDK client.  This injection is accomplished by defining a class @scala[extending]@java[implementing] @apidoc[akka.persistence.dynamodb.util.SDKClientMetricsProvider].

Your implementation must expose a single constructor with one argument: an `akka.actor.ClassicActorSystemProvider`.  Its `metricsPublisherFor` method will take the config path to the `client` section of this instance of the plugin @ref:[configuration](config.md#multiple-plugins).

The AWS SDK provides an implementation of `MetricPublisher` which publishes to [Amazon CloudWatch](https://docs.aws.amazon.com/cloudwatch/).  An `SDKClientMetricsProvider` providing [this `MetricPublisher`](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/metrics.html) with defaults would look like:

Scala
: @@snip [cloudwatch default](/docs/src/test/scala/docs/CloudWatchProvider.scala) { #cloudwatch-default }

Java
: @@snip [cloudwatch default](/docs/src/test/java/jdocs/CloudWatchWithDefaultConfigurationMetricsProvider.java) { #cloudwatch-default }

To register your provider implementation with the plugin, add its fully-qualified class name to the configuration path `akka.persistence.dynamodb.client.metrics-providers` (e.g. in `application.conf`):

```
akka.persistence.dynamodb.client.metrics-providers += domain.package.CloudWatchWithDefaultConfigurationMetricsProvider
```

If multiple providers are specified, they will automatically be combined into a "meta-provider" which provides a publisher which will publish using _all_ of the specified providers' respective publishers.

If implementing your own `MetricPublisher`, [Amazon recommends that care be taken to not block the thread calling the methods of the `MetricPublisher`](https://github.com/aws/aws-sdk-java-v2/blob/master/docs/design/core/metrics/Design.md#performance): all I/O and "heavy" computation should be performed asynchronously (e.g., since you have an `ActorSystem`, by `tell`ing the metrics to an actor) and control immediately returned to the caller.
