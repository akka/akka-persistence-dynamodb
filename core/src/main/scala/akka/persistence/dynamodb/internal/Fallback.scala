/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

// Possible candidate for inclusion in Akka Persistence
package akka.persistence.dynamodb.internal

import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * INTERNAL API API MAY CHANGE
 *
 * Plugin API for a fallback store for events and snapshots. The journal and snapshot store implementations may, if
 * configured to use an implementation of this API, use this fallback store to store events (resp. snapshots) while only
 * storing a reference (aka "breadcrumb") returned by the plugin in the journal (resp. snapshot store). The event or
 * snapshot can then be retrieved by providing the breadcrumb.
 *
 * Implementations must have a constructor taking an actor system, a Config object, and the config path
 *
 * @tparam Breadcrumb
 *   the type of breadcrumb emitted and aceepted by this store: this type should have an Akka serializer registered and
 *   may not be generic
 */
@ApiMayChange
@InternalApi
trait FallbackStore[Breadcrumb <: AnyRef] {
  def toBreadcrumb(maybeBreadcrumb: AnyRef): Option[Breadcrumb]

  def breadcumbClass: Class[Breadcrumb]

  def loadEvent(
      breadcrumb: Breadcrumb,
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[FallbackStore.EventFromFallback]]

  def saveEvent(
      persistenceId: String,
      seqNr: Long,
      serId: Int,
      serManifest: String,
      payload: Array[Byte]): Future[Breadcrumb]

  def loadSnapshot(
      breadcrumb: Breadcrumb,
      persistenceId: String,
      seqNr: Long): Future[Option[FallbackStore.SnapshotFromFallback]]

  def saveSnapshot(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Instant,
      eventTimestamp: Instant,
      serId: Int,
      serManifest: String,
      payload: Array[Byte],
      tags: Set[String],
      meta: Option[((Int, String), Array[Byte])]): Future[Breadcrumb]
}

/** INTERNAL API */
@InternalApi
object FallbackStore {

  /** API MAY CHANGE */
  @ApiMayChange
  final class EventFromFallback(val serId: Int, val serManifest: String, val payload: Option[Array[Byte]])

  /** API MAY CHANGE */
  @ApiMayChange
  final class SnapshotFromFallback(
      val seqNr: Long,
      val writeTimestamp: Instant,
      val eventTimestamp: Instant,
      val serId: Int,
      val serManifest: String,
      val payload: Array[Byte],
      val tags: Set[String],
      val meta: Option[((Int, String), Array[Byte])])

}

/** INTERNAL API */
@InternalApi
object FallbackStoreProvider extends ExtensionId[FallbackStoreProvider] {
  def createExtension(system: ActorSystem[_]): FallbackStoreProvider = new FallbackStoreProvider(system)

  /** Java API */
  def get(system: ActorSystem[_]): FallbackStoreProvider = apply(system)
}

/** INTERNAL API */
@InternalApi
class FallbackStoreProvider(system: ActorSystem[_]) extends Extension {
  private val fallbackStores = new ConcurrentHashMap[String, FallbackStore[AnyRef]]

  def fallbackStoreFor(configLocation: String): FallbackStore[AnyRef] = {
    val config = system.settings.config.getConfig(configLocation)
    val className = config.getString("class")
    if ((className eq null) || className.isEmpty) {
      throw new IllegalArgumentException(
        s"Plugin class name must be defined in config property [${configLocation}.class]")
    }

    val clazzTry = system.dynamicAccess.getClassFor[FallbackStore[AnyRef]](className)

    clazzTry match {
      case Success(clazz) =>
        fallbackStores.computeIfAbsent(
          configLocation,
          configLocation => {
            val argList: List[(Class[_], AnyRef)] =
              List(classOf[ActorSystem[_]] -> system, classOf[Config] -> config, classOf[String] -> configLocation)
            val plugin =
              system.dynamicAccess
                .createInstanceFor[FallbackStore[AnyRef]](clazz, argList)
                .recoverWith { case ex =>
                  Failure(
                    new RuntimeException(
                      s"Could not find constructor for FallbackStore plugin [$className] taking actor system, " +
                      "config, and config location",
                      ex))
                }
            plugin.get
          })

      case Failure(ex) =>
        throw new IllegalArgumentException(
          s"Plugin must be on classpath ([${configLocation}.class = \"$className\"])",
          ex)
    }
  }
}
