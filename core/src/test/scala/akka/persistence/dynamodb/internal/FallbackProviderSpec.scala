/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
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
    extends FallbackStore[String] {
  import FallbackStore._
  import InMemFallbackStore.Invocation

  implicit val systemForMat: ActorSystem[_] = system
  private val (queue, source) = Source.queue[Invocation](16).toMat(BroadcastHub.sink(1, 16))(Keep.both).run()

  def invocationStream: Source[Invocation, NotUsed] = source

  private val journal = mutable.HashMap.empty[String, mutable.HashMap[Long, EventFromFallback]]
  private val snapshotStore = mutable.HashMap.empty[String, SnapshotFromFallback]

  override def breadcumbClass: Class[String] = {
    queue.offer(Invocation("breadcrumbClass", Nil))
    classOf[String]
  }

  override def toBreadcrumb(maybeBreadcrumb: AnyRef): Option[String] = {
    queue.offer(Invocation("toBreadcrumb", Seq(maybeBreadcrumb)))
    maybeBreadcrumb match {
      case s: String => Some(s)
      case _         => None
    }
  }

  override def saveEvent(
      persistenceId: String,
      seqNr: Long,
      serId: Int,
      serManifest: String,
      payload: Array[Byte]): Future[String] = {
    queue.offer(Invocation("saveEvent", Seq(persistenceId, seqNr, serId, serManifest, payload)))

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
      includePayload: Boolean): Future[Option[FallbackStore.EventFromFallback]] = {
    queue.offer(Invocation("loadEvent", Seq(breadcrumb, persistenceId, seqNr, includePayload)))

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
      Invocation(
        "saveSnapshot",
        Seq(persistenceId, seqNr, writeTimestamp, eventTimestamp, serId, serManifest, payload, tags, meta)))

    val snapshot =
      new SnapshotFromFallback(seqNr, writeTimestamp, eventTimestamp, serId, serManifest, payload, tags, meta)

    snapshotStore.update(persistenceId, snapshot)

    Future.successful("breadcrumb")
  }

  override def loadSnapshot(
      breadcrumb: String,
      persistenceId: String,
      seqNr: Long): Future[Option[SnapshotFromFallback]] = {
    queue.offer(Invocation("loadSnapshot", Seq(breadcrumb, persistenceId, seqNr)))

    if (breadcrumb == "breadcrumb") {
      Future.successful(snapshotStore.get(persistenceId))
    } else
      Future.failed(new AssertionError(
        "This fallback store only accepts \"breadcrumb\": the snapshot DAO must have improperly manipulated the returned breadcrumb!"))
  }
}

object InMemFallbackStore {
  case class Invocation(method: String, args: Seq[Any])
}

class ImproperFallbackStore extends FallbackStore[AnyRef] {
  def toBreadcrumb(maybeBreadcrumb: AnyRef): Option[AnyRef] = None

  def breadcumbClass: Class[AnyRef] = classOf[AnyRef]

  def loadEvent(
      breadcrumb: AnyRef,
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[FallbackStore.EventFromFallback]] = fail

  def saveEvent(
      persistenceId: String,
      seqNr: Long,
      serId: Int,
      serManifest: String,
      payload: Array[Byte]): Future[AnyRef] = fail

  def loadSnapshot(
      breadcrumb: AnyRef,
      persistenceId: String,
      seqNr: Long): Future[Option[FallbackStore.SnapshotFromFallback]] = fail

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
      val fallbackStore = FallbackStoreProvider(testkit.system).fallbackStoreFor("fallback-plugin")

      fallbackStore shouldBe an[InMemFallbackStore]
    }

    "memoize constructed instances" in withActorTestKit("""
      fallback-plugin {
        class = "akka.persistence.dynamodb.internal.InMemFallbackStore"
      }
      """) { testkit =>
      val firstFallbackStore = FallbackStoreProvider(testkit.system).fallbackStoreFor("fallback-plugin")
      val secondFallbackStore = FallbackStoreProvider(testkit.system).fallbackStoreFor("fallback-plugin")

      firstFallbackStore shouldBe secondFallbackStore
    }

    "require that fallback store implementations be bound by config" in withActorTestKit("""
      fallback-plugin {
        class = ""
      }
      """) { testkit =>
      an[IllegalArgumentException] shouldBe thrownBy {
        FallbackStoreProvider(testkit.system).fallbackStoreFor("fallback-plugin")
      }
    }

    "not allow fallback store implementations without a proper constructor" in withActorTestKit("""
      fallback-plugin {
        class = "akka.persistence.dynamodb.internal.ImproperFallbackStore"
      }
      """) { testkit =>
      val re = the[RuntimeException] thrownBy {
        FallbackStoreProvider(testkit.system).fallbackStoreFor("fallback-plugin")
      }

      re.getMessage should include("Could not find constructor for FallbackStore plugin [")
      re.getMessage should include("] taking actor system, config, and config location")
    }
  }

  def withActorTestKit(conf: String = "")(test: ActorTestKit => Unit): Unit = {
    val config = ConfigFactory.load(ConfigFactory.parseString(conf))
    val testkit = ActorTestKit(ActorTestKitBase.testNameFromCallStack(), config)
    try test(testkit)
    finally testkit.shutdownTestKit()
  }
}
