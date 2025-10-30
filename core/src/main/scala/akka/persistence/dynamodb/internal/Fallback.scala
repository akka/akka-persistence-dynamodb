/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

// Possible candidate for inclusion in Akka Persistence
package akka.persistence.dynamodb.internal

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.actor.typed.DispatcherSelector
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Failure
import scala.util.Success

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext

/**
 * INTERNAL API API MAY CHANGE
 *
 * Do not inherit directly from this interface. Instead inherit one or both of `EventFallbackStore` or
 * `SnapshotFallbackStore`
 */
@ApiMayChange
@InternalApi
trait CommonFallbackStore[Breadcrumb <: AnyRef] {
  def toBreadcrumb(maybeBreadcrumb: AnyRef): Option[Breadcrumb]
  def breadcrumbClass: Class[Breadcrumb]

  def close(): Unit
}

/**
 * INTERNAL API API MAY CHANGE
 *
 * Plugin API for a fallback store for events. The journal implementations may, if configured to use an implementation
 * of this API, use this fallback store to store events while only storing a reference (aka "breadcrumb") returned by
 * the plugin in the journal. The event can then be retrieved by providing the breadcrumb.
 *
 * Implementations must have a constructor taking an actor system, a Config object, and the config path
 *
 * @tparam Breadcrumb
 *   the type of breadcrumb emitted and aceepted by this store: this type should have an Akka serializer registered and
 *   may not be generic
 */
@ApiMayChange
@InternalApi
trait EventFallbackStore[Breadcrumb <: AnyRef] extends CommonFallbackStore[Breadcrumb] {

  def loadEvent(
      breadcrumb: Breadcrumb,
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[EventFallbackStore.EventFromFallback]]

  def saveEvent(
      persistenceId: String,
      seqNr: Long,
      serId: Int,
      serManifest: String,
      payload: Array[Byte]): Future[Breadcrumb]
}

@InternalApi
object EventFallbackStore {

  /** API MAY CHANGE */
  @ApiMayChange
  final class EventFromFallback(val serId: Int, val serManifest: String, val payload: Option[Array[Byte]])
}

/**
 * INTERNAL API API MAY CHANGE
 *
 * Plugin API for a fallback store for snapshots. The snapshot store implementations may, if configured to use an
 * implementation of this API, use this fallback store to store snapshots while only storing a reference (aka
 * "breadcrumb") returned by the plugin in the snapshot store. The snapshot can then be retrieved by providing the
 * breadcrumb.
 *
 * Implementations must have a constructor taking an actor system, a Config object, and the config path
 *
 * @tparam Breadcrumb
 *   the type of breadcrumb emitted and aceepted by this store: this type should have an Akka serializer registered and
 *   may not be generic
 */
@ApiMayChange
@InternalApi
trait SnapshotFallbackStore[Breadcrumb <: AnyRef] extends CommonFallbackStore[Breadcrumb] {

  def loadSnapshot(
      breadcrumb: Breadcrumb,
      persistenceId: String,
      seqNr: Long): Future[Option[SnapshotFallbackStore.SnapshotFromFallback]]

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

@InternalApi
object SnapshotFallbackStore {

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
  CoordinatedShutdown(system)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close fallback stores") { () =>
      import system.executionContext

      val blockingDispatcher = system.dispatchers.lookup(DispatcherSelector.blocking())

      Future
        .sequence(
          fallbackStores.asScala.valuesIterator
            .flatMap {
              case (efs, _) if efs ne null => Some(efs)
              case (_, sfs) if sfs ne null => Some(sfs)
              case _                       => None
            }
            .map { fs => Future { fs.close(); Done }(blockingDispatcher) }
            .toList)
        .map { _ => Done }(ExecutionContext.parasitic)
    }

  def eventFallbackStoreFor(configLocation: String): EventFallbackStore[AnyRef] = {
    val storePair =
      fallbackStores.computeIfAbsent(configLocation, configLocation => pair(constructFallbackStore(configLocation)))

    storePair match {
      case (efs: EventFallbackStore[_], _) if efs.isInstanceOf[EventFallbackStore[_]] => efs
      case _ =>
        throw new RuntimeException(
          s"Config location $configLocation refers to a fallback store which is not an EventFallbackStore")
    }
  }

  def snapshotFallbackStoreFor(configLocation: String): SnapshotFallbackStore[AnyRef] = {
    val storePair =
      fallbackStores.computeIfAbsent(configLocation, configLocation => pair(constructFallbackStore(configLocation)))

    storePair match {
      case (_, sfs: SnapshotFallbackStore[_]) if sfs.isInstanceOf[SnapshotFallbackStore[_]] => sfs
      case _ =>
        throw new RuntimeException(
          s"Config location $configLocation refers to a fallback store which is not a SnapshotFallbackStore")
    }
  }

  private def pair(
      constructed: CommonFallbackStore[AnyRef]): (EventFallbackStore[AnyRef], SnapshotFallbackStore[AnyRef]) = {
    val eventFallback = constructed match {
      case efs: EventFallbackStore[_] => efs
      case _                          => null
    }

    val snapshotFallback = constructed match {
      case sfs: SnapshotFallbackStore[_] => sfs
      case _                             => null
    }

    (eventFallback, snapshotFallback)
  }

  private def constructFallbackStore(configLocation: String): CommonFallbackStore[AnyRef] = {
    val config = system.settings.config.getConfig(configLocation)
    val className = config.getString("class")
    if ((className eq null) || className.isEmpty) {
      throw new IllegalArgumentException(
        s"Plugin class name must be defined in config property [${configLocation}.class]")
    }

    system.dynamicAccess.getClassFor[CommonFallbackStore[AnyRef]](className) match {
      case Success(clazz) =>
        val argList: List[(Class[_], AnyRef)] =
          List(classOf[ActorSystem[_]] -> system, classOf[Config] -> config, classOf[String] -> configLocation)

        val plugin =
          system.dynamicAccess
            .createInstanceFor[CommonFallbackStore[AnyRef]](clazz, argList)
            .recoverWith { case ex =>
              Failure(new RuntimeException(s"Could not create instance of fallback store plugin [$className]", ex))
            }

        plugin.get

      case Failure(ex) =>
        throw new IllegalArgumentException(
          s"Plugin must be on classpath (${configLocation}.class = \"$className\"])",
          ex)
    }
  }

  /** INTERNAL API */
  @InternalApi
  final private[akka] def getFallbackStore(id: String): Option[CommonFallbackStore[AnyRef]] =
    fallbackStores.get(id) match {
      case null | (null, null) => None
      case (null, snap)        => Some(snap)
      case (evt, _)            => Some(evt)
    }

  private val fallbackStores =
    new ConcurrentHashMap[String, (EventFallbackStore[AnyRef], SnapshotFallbackStore[AnyRef])]
}
