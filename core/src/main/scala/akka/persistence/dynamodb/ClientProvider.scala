/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import java.net.URI
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object ClientProvider extends ExtensionId[ClientProvider] {
  def createExtension(system: ActorSystem[_]): ClientProvider = new ClientProvider(system)

  // Java API
  def get(system: ActorSystem[_]): ClientProvider = apply(system)
}
class ClientProvider(system: ActorSystem[_]) extends Extension {
  private val clients = new ConcurrentHashMap[String, DynamoDbAsyncClient]
  private val clientSettings = new ConcurrentHashMap[String, ClientSettings]

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
        createClient(settings)
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

  private def createClient(settings: ClientSettings): DynamoDbAsyncClient = {
    // FIXME more config
    DynamoDbAsyncClient.builder
      .httpClientBuilder(NettyNioAsyncHttpClient.builder)
      .endpointOverride(URI.create(s"http://${settings.host}:${settings.port}"))
      .region(Region.US_WEST_2)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummyKey", "dummySecret")))
      .build()
  }

}
