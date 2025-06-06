/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.util

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.jdk.OptionConverters._

import akka.actor.ClassicActorSystemProvider
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.ActorTestKitBase
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.metrics.MetricCollection
import software.amazon.awssdk.metrics.MetricPublisher
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.LegacyRetryStrategy
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

class ClientProviderSpec extends AnyWordSpec with Matchers with OptionValues {

  "ClientProvider" should {

    "create client with default settings" in withActorTestKit("""
      akka.persistence.dynamodb.client {
        # region usually picked up automatically, set manually for test
        region = "us-east-1"
      }
      """) { testKit =>
      val clientConfigLocation = "akka.persistence.dynamodb.client"
      val settings = ClientProvider(testKit.system).clientSettingsFor(clientConfigLocation)
      val client = ClientProvider(testKit.system).clientFor(clientConfigLocation)
      client shouldBe a[DynamoDbAsyncClient]

      val clientConfiguration = client.serviceClientConfiguration
      clientConfiguration.region shouldBe Region.US_EAST_1

      val overrideConfiguration = clientConfiguration.overrideConfiguration
      overrideConfiguration.apiCallTimeout.toScala shouldBe Some(9.seconds.toJava)
      overrideConfiguration.apiCallAttemptTimeout.toScala shouldBe None

      val httpSettings = settings.http
      httpSettings.maxConcurrency shouldBe 50
      httpSettings.maxPendingConnectionAcquires shouldBe 10000
      httpSettings.readTimeout shouldBe 30.seconds
      httpSettings.writeTimeout shouldBe 30.seconds
      httpSettings.connectionTimeout shouldBe 2.seconds
      httpSettings.connectionAcquisitionTimeout shouldBe 10.seconds
      httpSettings.connectionTimeToLive shouldBe 0.seconds
      httpSettings.useIdleConnectionReaper shouldBe true
      httpSettings.connectionMaxIdleTime shouldBe 60.seconds
      httpSettings.tlsNegotiationTimeout shouldBe 5.seconds
      httpSettings.tcpKeepAlive shouldBe false

      val retryStrategy = overrideConfiguration.retryStrategy.toScala.value
      retryStrategy shouldBe a[StandardRetryStrategy]
      retryStrategy.maxAttempts shouldBe 3

      val compressionConfiguration = overrideConfiguration.compressionConfiguration.toScala.value
      compressionConfiguration.requestCompressionEnabled shouldBe true
      compressionConfiguration.minimumCompressionThresholdInBytes shouldBe 10240
    }

    "create client with a MetricPublisher" in withActorTestKit("""
      akka.persistence.dynamodb.client {
        region = "us-east-1"
        metrics-providers += akka.persistence.dynamodb.util.TestNoopMetricsProvider
      }
    """) { testKit =>
      val clientConfigLocation = "akka.persistence.dynamodb.client"
      val client = ClientProvider(testKit.system).clientFor(clientConfigLocation)

      val clientConfiguration = client.serviceClientConfiguration
      val overrideConfiguration = clientConfiguration.overrideConfiguration
      val metricPublishers = overrideConfiguration.metricPublishers.asScala.toSeq
      metricPublishers.size shouldBe 1
      metricPublishers should contain(TestNoopMetricsProvider.publisher)
    }

    "create client with configured settings" in withActorTestKit("""
      akka.persistence.dynamodb.client {
        call-timeout = 3 seconds
        call-attempt-timeout = 500 millis

        http {
          max-concurrency = 100
          max-pending-connection-acquires = 100000
          read-timeout = 10 seconds
          write-timeout = 10 seconds
          connection-timeout = 5 seconds
          connection-acquisition-timeout = 5 seconds
          connection-time-to-live = 30 seconds
          use-idle-connection-reaper = false
          connection-max-idle-time = 30 seconds
          tls-negotiation-timeout = 10 seconds
          tcp-keep-alive = true
        }

        retry-strategy {
          retry-mode = legacy
          max-attempts = 5
        }

        compression {
          enabled = off
          threshold = 20 KiB
        }

        # region usually picked up automatically, set manually for test
        region = "us-east-1"
      }
      """) { testKit =>
      val clientConfigLocation = "akka.persistence.dynamodb.client"
      val settings = ClientProvider(testKit.system).clientSettingsFor(clientConfigLocation)
      val client = ClientProvider(testKit.system).clientFor(clientConfigLocation)
      client shouldBe a[DynamoDbAsyncClient]

      val clientConfiguration = client.serviceClientConfiguration
      clientConfiguration.region shouldBe Region.US_EAST_1

      val overrideConfiguration = clientConfiguration.overrideConfiguration
      overrideConfiguration.apiCallTimeout.toScala shouldBe Some(3.seconds.toJava)
      overrideConfiguration.apiCallAttemptTimeout.toScala shouldBe Some(500.millis.toJava)

      val httpSettings = settings.http
      httpSettings.maxConcurrency shouldBe 100
      httpSettings.maxPendingConnectionAcquires shouldBe 100000
      httpSettings.readTimeout shouldBe 10.seconds
      httpSettings.writeTimeout shouldBe 10.seconds
      httpSettings.connectionTimeout shouldBe 5.seconds
      httpSettings.connectionAcquisitionTimeout shouldBe 5.seconds
      httpSettings.connectionTimeToLive shouldBe 30.seconds
      httpSettings.useIdleConnectionReaper shouldBe false
      httpSettings.connectionMaxIdleTime shouldBe 30.seconds
      httpSettings.tlsNegotiationTimeout shouldBe 10.seconds
      httpSettings.tcpKeepAlive shouldBe true

      val retryStrategy = overrideConfiguration.retryStrategy.toScala.value
      retryStrategy shouldBe a[LegacyRetryStrategy]
      retryStrategy.maxAttempts shouldBe 5

      val compressionConfiguration = overrideConfiguration.compressionConfiguration.toScala.value
      compressionConfiguration.requestCompressionEnabled shouldBe false
      compressionConfiguration.minimumCompressionThresholdInBytes shouldBe 20480
    }

    "create client with no retry strategy if configured" in withActorTestKit("""
      akka.persistence.dynamodb.client {
        retry-strategy {
          enabled = off
        }
        
        # region usually picked up automatically, set manually for test
        region = "us-east-1"
      }
      """) { testKit =>
      val clientConfigLocation = "akka.persistence.dynamodb.client"
      val client = ClientProvider(testKit.system).clientFor(clientConfigLocation)
      client shouldBe a[DynamoDbAsyncClient]

      val overrideConfiguration = client.serviceClientConfiguration.overrideConfiguration
      val retryStrategy = overrideConfiguration.retryStrategy.toScala.value
      retryStrategy shouldBe a[StandardRetryStrategy]
      retryStrategy.maxAttempts shouldBe 1
    }

    "create client using deprecated retry policy settings" in withActorTestKit("""
      akka.persistence.dynamodb.client {
        retry-policy {
          retry-mode = legacy
          num-retries = 9
        }

        # region usually picked up automatically, set manually for test
        region = "us-east-1"
      }
      """) { testKit =>
      import testKit.system

      val clientConfigLocation = "akka.persistence.dynamodb.client"
      val client =
        LoggingTestKit
          .warn(
            "Configuration for `akka.persistence.dynamodb.client.retry-policy` is deprecated. Use `retry-strategy` instead.")
          .expect {
            ClientProvider(testKit.system).clientFor(clientConfigLocation)
          }
      client shouldBe a[DynamoDbAsyncClient]

      val overrideConfiguration = client.serviceClientConfiguration.overrideConfiguration
      val retryStrategy = overrideConfiguration.retryStrategy.toScala.value
      retryStrategy shouldBe a[LegacyRetryStrategy]
      retryStrategy.maxAttempts shouldBe 10
    }

    "create client with no retry strategy if configured using deprecated retry policy settings" in withActorTestKit("""
      akka.persistence.dynamodb.client {
        retry-policy {
          enabled = off
        }

        # region usually picked up automatically, set manually for test
        region = "us-east-1"
      }
      """) { testKit =>
      import testKit.system

      val clientConfigLocation = "akka.persistence.dynamodb.client"
      val client =
        LoggingTestKit
          .warn(
            "Configuration for `akka.persistence.dynamodb.client.retry-policy` is deprecated. Use `retry-strategy` instead.")
          .expect {
            ClientProvider(testKit.system).clientFor(clientConfigLocation)
          }
      client shouldBe a[DynamoDbAsyncClient]

      val overrideConfiguration = client.serviceClientConfiguration.overrideConfiguration
      val retryStrategy = overrideConfiguration.retryStrategy.toScala.value
      retryStrategy shouldBe a[StandardRetryStrategy]
      retryStrategy.maxAttempts shouldBe 1
    }
  }

  def withActorTestKit(conf: String = "")(test: ActorTestKit => Unit): Unit = {
    val config = ConfigFactory.load(ConfigFactory.parseString(conf))
    val testKit = ActorTestKit(ActorTestKitBase.testNameFromCallStack(), config)
    try test(testKit)
    finally testKit.shutdownTestKit()
  }

}

class TestNoopMetricsProvider(system: ClassicActorSystemProvider) extends AWSClientMetricsProvider {
  def metricPublisherFor(configLocation: String): MetricPublisher = TestNoopMetricsProvider.publisher
}

object TestNoopMetricsProvider {
  val publisher =
    new MetricPublisher {
      def publish(collection: MetricCollection): Unit = ()
      def close(): Unit = ()
    }
}
