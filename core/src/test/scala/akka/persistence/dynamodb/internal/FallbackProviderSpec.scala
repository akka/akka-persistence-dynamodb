/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.internal

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.Future

import java.time.Instant
import akka.stream.scaladsl.Keep
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.ConfigFactory
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.ActorTestKitBase

final class InMemFallbackStore(system: ActorSystem[_], config: Config, configLocation: String)
    extends EventFallbackStore[String]
    with SnapshotFallbackStore[String] {
  import EventFallbackStore._
  import SnapshotFallbackStore._
  import InMemFallbackStore._

  implicit val systemForMat: ActorSystem[_] = system
  private val (queue, source) = Source.queue[Invocation](16).toMat(BroadcastHub.sink(1, 16))(Keep.both).run()

  def invocationStream: Source[Invocation, NotUsed] = source

  private val journal = mutable.HashMap.empty[String, mutable.HashMap[Long, EventFromFallback]]
  private val snapshotStore = mutable.HashMap.empty[String, SnapshotFromFallback]

  override def breadcrumbClass: Class[String] = {
    queue.offer(BreadcrumbClass)
    classOf[String]
  }

  override def toBreadcrumb(maybeBreadcrumb: AnyRef): Option[String] = {
    queue.offer(ToBreadcrumb(maybeBreadcrumb))
    maybeBreadcrumb match {
      case s: String => Some(s)
      case _         => None
    }
  }

  override def close(): Unit = {}

  override def saveEvent(
      persistenceId: String,
      seqNr: Long,
      serId: Int,
      serManifest: String,
      payload: Array[Byte]): Future[String] = {
    queue.offer(SaveEvent(persistenceId, seqNr, serId, serManifest, payload))

    val evt = new EventFromFallback(serId, serManifest, Some(payload))

    journal.updateWith(persistenceId) { maybePreviousEvents =>
      maybePreviousEvents match {
        case None => Some(mutable.HashMap(seqNr -> evt))
        case ret @ Some(previousEvents) =>
          previousEvents.update(seqNr, evt)
          ret
      }
    }

    Future.successful("breadcrumb")
  }

  override def loadEvent(
      breadcrumb: String,
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[EventFallbackStore.EventFromFallback]] = {
    queue.offer(LoadEvent(breadcrumb, persistenceId, seqNr, includePayload))

    if (breadcrumb == "breadcrumb") {
      val foundEvt = journal.get(persistenceId).flatMap { log => log.get(seqNr) }
      Future.successful(
        if (includePayload) foundEvt
        else foundEvt.map { evt => new EventFromFallback(evt.serId, evt.serManifest, None) })
    } else
      Future.failed(new AssertionError(
        "This fallback store only accepts \"breadcrumb\": the journal DAO must have improperly manipulated the returned breadcrumb!"))
  }

  override def saveSnapshot(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Instant,
      eventTimestamp: Instant,
      serId: Int,
      serManifest: String,
      payload: Array[Byte],
      tags: Set[String],
      meta: Option[((Int, String), Array[Byte])]): Future[String] = {
    queue.offer(
      SaveSnapshot(persistenceId, seqNr, writeTimestamp, eventTimestamp, serId, serManifest, payload, tags, meta))

    val snapshot =
      new SnapshotFromFallback(seqNr, writeTimestamp, eventTimestamp, serId, serManifest, payload, tags, meta)

    snapshotStore.update(persistenceId, snapshot)

    Future.successful("breadcrumb")
  }

  override def loadSnapshot(
      breadcrumb: String,
      persistenceId: String,
      seqNr: Long): Future[Option[SnapshotFallbackStore.SnapshotFromFallback]] = {
    queue.offer(LoadSnapshot(breadcrumb, persistenceId, seqNr))

    if (breadcrumb == "breadcrumb") {
      Future.successful(snapshotStore.get(persistenceId))
    } else
      Future.failed(new AssertionError(
        "This fallback store only accepts \"breadcrumb\": the snapshot DAO must have improperly manipulated the returned breadcrumb!"))
  }
}

object InMemFallbackStore {
  sealed trait Invocation

  case object BreadcrumbClass extends Invocation

  case class ToBreadcrumb(maybeBreadcrumb: AnyRef) extends Invocation

  case class SaveEvent(persistenceId: String, seqNr: Long, serId: Int, serManifest: String, payload: Array[Byte])
      extends Invocation

  case class LoadEvent(breadcrumb: String, persistenceId: String, seqNr: Long, includePayload: Boolean)
      extends Invocation

  case class SaveSnapshot(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Instant,
      eventTimestamp: Instant,
      serId: Int,
      serManifest: String,
      payload: Array[Byte],
      tags: Set[String],
      meta: Option[((Int, String), Array[Byte])])
      extends Invocation
  case class LoadSnapshot(breadcrumb: String, persistenceId: String, seqNr: Long) extends Invocation
}

class ImproperFallbackStore extends EventFallbackStore[AnyRef] with SnapshotFallbackStore[AnyRef] {
  def toBreadcrumb(maybeBreadcrumb: AnyRef): Option[AnyRef] = None

  def breadcrumbClass: Class[AnyRef] = classOf[AnyRef]

  def close(): Unit = {}

  def loadEvent(
      breadcrumb: AnyRef,
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[EventFallbackStore.EventFromFallback]] = fail

  def saveEvent(
      persistenceId: String,
      seqNr: Long,
      serId: Int,
      serManifest: String,
      payload: Array[Byte]): Future[AnyRef] = fail

  def loadSnapshot(
      breadcrumb: AnyRef,
      persistenceId: String,
      seqNr: Long): Future[Option[SnapshotFallbackStore.SnapshotFromFallback]] = fail

  def saveSnapshot(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Instant,
      eventTimestamp: Instant,
      serId: Int,
      serManifest: String,
      payload: Array[Byte],
      tags: Set[String],
      meta: Option[((Int, String), Array[Byte])]): Future[AnyRef] = fail

  def fail: Future[Nothing] = Future.failed(new NotImplementedError)
}

class FallbackStoreProviderSpec extends AnyWordSpec with Matchers {
  "FallbackProvider" should {
    "create fallback client with specified plugin class" in withActorTestKit("""
      fallback-plugin {
        class = "akka.persistence.dynamodb.internal.InMemFallbackStore"
      }
      """) { testkit =>
      val eventFallbackStore = FallbackStoreProvider(testkit.system).eventFallbackStoreFor("fallback-plugin")
      val snapshotFallbackStore = FallbackStoreProvider(testkit.system).snapshotFallbackStoreFor("fallback-plugin")

      eventFallbackStore shouldBe an[InMemFallbackStore]
      snapshotFallbackStore shouldBe an[InMemFallbackStore]
    }

    "memoize constructed instances" in withActorTestKit("""
      fallback-plugin {
        class = "akka.persistence.dynamodb.internal.InMemFallbackStore"
      }
      """) { testkit =>
      val firstFallbackStore = FallbackStoreProvider(testkit.system).eventFallbackStoreFor("fallback-plugin")
      val secondFallbackStore = FallbackStoreProvider(testkit.system).eventFallbackStoreFor("fallback-plugin")

      firstFallbackStore shouldBe secondFallbackStore
    }

    "require that fallback store implementations be bound by config" in withActorTestKit("""
      fallback-plugin {
        class = ""
      }
      """) { testkit =>
      an[IllegalArgumentException] shouldBe thrownBy {
        FallbackStoreProvider(testkit.system).snapshotFallbackStoreFor("fallback-plugin")
      }
    }

    "not allow fallback store implementations without a proper constructor" in withActorTestKit("""
      fallback-plugin {
        class = "akka.persistence.dynamodb.internal.ImproperFallbackStore"
      }
      """) { testkit =>
      val re = the[RuntimeException] thrownBy {
        FallbackStoreProvider(testkit.system).eventFallbackStoreFor("fallback-plugin")
      }

      re.getMessage should include("Could not create instance of fallback store plugin [")
    }
  }

  def withActorTestKit(conf: String = "")(test: ActorTestKit => Unit): Unit = {
    val config = ConfigFactory.load(ConfigFactory.parseString(conf))
    val testkit = ActorTestKit(ActorTestKitBase.testNameFromCallStack(), config)
    try test(testkit)
    finally testkit.shutdownTestKit()
  }
}
