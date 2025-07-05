/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.s3fallback

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.persistence.dynamodb.internal.EventFallbackStore
import akka.persistence.dynamodb.internal.SnapshotFallbackStore
import akka.persistence.typed.PersistenceId
import akka.util.Base62
import akka.util.ConstantFun
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.FutureConverters._
import scala.util.Failure
import scala.util.Success

import java.time.Instant
import java.time.Clock
import akka.actor.typed.Props

final class S3Fallback(system: ActorSystem[_], config: Config, configLocation: String)
    extends EventFallbackStore[String]
    with SnapshotFallbackStore[String] {
  import EventFallbackStore._
  import SnapshotFallbackStore._
  import S3Fallback._
  import system.executionContext

  override def toBreadcrumb(maybeBreadcrumb: AnyRef): Option[String] =
    maybeBreadcrumb match {
      case s: String => Some(s)
      case _         => None
    }

  override def breadcrumbClass: Class[String] = classOf[String]

  override def close(): Unit = {
    log.info("Closing S3 fallback")
    warmer.foreach(_ ! Closing)
    client.close()
  }

  override def loadEvent(
      breadcrumb: String,
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[EventFromFallback]] =
    eventFallbackInitialFuture.flatMap { _ => _loadEvent(breadcrumb, persistenceId, seqNr, includePayload) }

  private def _loadEvent(
      bucket: String, // the breadcrumb
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[EventFromFallback]] = {
    if (log.isDebugEnabled) {
      log.debug(s"Retrieving event from bucket [$bucket] for persistenceId [$persistenceId] at seqNr [$seqNr]")
    }

    val eventFolder = seqNr / eventsPerFolder
    val key = s"${keyForPid(persistenceId)}/$eventFolder/$seqNr"

    if (includePayload) {
      val req = GetObjectRequest.builder.bucket(bucket).key(key).build()

      client
        .getObject(req, AsyncResponseTransformer.toBytes[GetObjectResponse])
        .asScala
        .flatMap { respBytes =>
          val response = respBytes.response
          // AWS SDK constructs the byte array and promises not to modify
          val bytes = respBytes.asByteArrayUnsafe

          val metadata = response.metadata.asScala.toMap

          event(Some(bytes), metadata)
        }
        .andThen {
          case Success(_) =>
            if (log.isDebugEnabled) {
              log.debug(s"Retrieved event from bucket [$bucket] for persistenceId [$persistenceId] at seqNr [$seqNr]")
            }

          case Failure(ex) =>
            log.error(
              "Failed to load event from bucket [{}] for persistenceId [{}] at seqNr [{}]",
              bucket,
              persistenceId,
              seqNr,
              ex)
        }
        .recover { case _: NoSuchKeyException | _: NoSuchBucketException =>
          None
        }(ExecutionContext.parasitic)
    } else {
      client
        .headObject(HeadObjectRequest.builder.bucket(bucket).key(key).build)
        .asScala
        .flatMap { response =>
          event(None, response.metadata.asScala.toMap)
        }
    }
  }

  private def event(bytesOpt: Option[Array[Byte]], metadata: Map[String, String]): Future[Option[EventFromFallback]] = {
    val serId = metadata.get("akka-ser-id")
    val serManifest = metadata.get("akka-ser-manifest")

    val attrs = Iterator(serId, serManifest).flatten.toIndexedSeq

    if (attrs.size == 2) {
      Future.successful(Some(new EventFromFallback(attrs(0).toInt, attrs(1), bytesOpt)))
    } else {
      Future.failed(new IllegalStateException("No Akka event metadata found"))
    }
  }

  def saveEvent(
      persistenceId: String,
      seqNr: Long,
      serId: Int,
      serManifest: String,
      payload: Array[Byte]): Future[String] =
    eventFallbackInitialFuture.flatMap { _ => _saveEvent(persistenceId, seqNr, serId, serManifest, payload) }

  private def _saveEvent(
      persistenceId: String,
      seqNr: Long,
      serId: Int,
      serManifest: String,
      payload: Array[Byte]): Future[String] = {
    val bucket = settings.eventsBucket

    if (log.isDebugEnabled) {
      log.debug(
        s"Saving event to bucket [$bucket] for persistenceId [$persistenceId] at seqNr [$seqNr] with size [${payload.length}] bytes")
    }

    val eventFolder = seqNr / eventsPerFolder
    val key = s"${keyForPid(persistenceId)}/$eventFolder/$seqNr"

    val req = PutObjectRequest.builder
      .bucket(bucket)
      .key(key)
      .metadata(Map("akka-ser-id" -> serId.toString, "akka-ser-manifest" -> serManifest).asJava)
      .contentType("application/octet-stream")
      .build()

    client
      .putObject(req, AsyncRequestBody.fromBytes(payload))
      .asScala
      .andThen {
        case Success(_) =>
          if (log.isDebugEnabled) {
            log.debug(s"Saved event to bucket [$bucket] for persistenceId [$persistenceId] at seqNr [$seqNr]")
          }

        case Failure(ex) =>
          log.error(
            "Failed to save event to bucket [{}] for persistenceId [{}] at seqNr [{}]",
            bucket,
            persistenceId,
            seqNr,
            ex)
      }
      .map(_ => bucket)(ExecutionContext.parasitic)
  }

  override def loadSnapshot(
      breadcrumb: String,
      persistenceId: String,
      seqNr: Long): Future[Option[SnapshotFromFallback]] =
    snapshotFallbackInitialFuture.flatMap { _ => _loadSnapshot(breadcrumb, persistenceId, seqNr) }

  private def _loadSnapshot(
      bucket: String, // aka breadcrumb
      pid: String,
      seqNr: Long): Future[Option[SnapshotFromFallback]] = {
    if (log.isDebugEnabled) {
      log.debug(s"Loading snapshot from bucket [$bucket] for persistenceId [$pid] seqNr [$seqNr]")
    }

    val req = GetObjectRequest.builder
      .bucket(bucket)
      .key(keyForPid(pid))
      .build()

    client
      .getObject(req, AsyncResponseTransformer.toBytes[GetObjectResponse])
      .asScala
      .flatMap { respBytes =>
        val response = respBytes.response
        // AWS SDK constructs the byte array and promises not to modify
        val bytes = respBytes.asByteArrayUnsafe

        val metadata = response.metadata.asScala

        val seqNr = metadata.get("akka-entity-seq-nr")
        val writeTimestamp = metadata.get("akka-write-timestamp")
        val serId = metadata.get("akka-ser-id")
        val serManifest = metadata.get("akka-ser-manifest")
        val eventTimestamp = metadata.get("akka-event-timestamp")

        val baseAttrs = Iterator(seqNr, writeTimestamp, serId, serManifest, eventTimestamp).flatten.toIndexedSeq

        if (baseAttrs.size == 5) {
          val metaSerId = metadata.get("akka-meta-ser-id")
          val metaSerManifest = metadata.get("akka-meta-ser-manifest")
          val metaPayload = metadata.get("akka-meta-payload")

          val metaAttrs = Iterator(metaSerId, metaSerManifest, metaPayload).flatten.toIndexedSeq

          if (metaAttrs.isEmpty || metaAttrs.size == 3) {
            val tags = metadata.iterator.flatMap { case (k, v) =>
              if (k.startsWith("akkaTag-") && k.charAt(8).isDigit) Some(v)
              else None
            }.toSet

            Future.successful(
              Some(
                new SnapshotFromFallback(
                  seqNr = baseAttrs(0).toLong,
                  writeTimestamp = Instant.parse(baseAttrs(1)),
                  eventTimestamp = Instant.parse(baseAttrs(4)),
                  payload = bytes,
                  serId = baseAttrs(2).toInt,
                  serManifest = baseAttrs(3),
                  tags = tags,
                  meta =
                    if (metaAttrs.isEmpty) None
                    else Some((metaAttrs(0).toInt -> metaAttrs(1)) -> Base62.decode(metaAttrs(2))))))
          } else Future.failed(new IllegalStateException("Incomplete Akka snapshot metadata found"))
        } else {
          Future.failed(new IllegalStateException("No Akka snapshot metadata found"))
        }
      }
      .andThen {
        case Success(_) =>
          if (log.isDebugEnabled) {
            log.debug(s"Loaded snapshot from bucket [$bucket] for persistenceId [$pid] seqNr [$seqNr]")
          }

        case Failure(ex) =>
          log.error(
            "Failed to load snapshot from bucket [{}] for persistenceId [{}] seqNr [{}]",
            bucket,
            pid,
            seqNr,
            ex)
      }
      .recover { case _: NoSuchKeyException | _: NoSuchBucketException =>
        None
      }(ExecutionContext.parasitic)
  }

  def saveSnapshot(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Instant,
      eventTimestamp: Instant,
      serId: Int,
      serManifest: String,
      payload: Array[Byte],
      tags: Set[String],
      meta: Option[((Int, String), Array[Byte])]): Future[String] =
    snapshotFallbackInitialFuture.flatMap { _ =>
      _saveSnapshot(persistenceId, seqNr, writeTimestamp, eventTimestamp, serId, serManifest, payload, tags, meta)
    }

  private def _saveSnapshot(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Instant,
      eventTimestamp: Instant,
      serId: Int,
      serManifest: String,
      payload: Array[Byte],
      tags: Set[String],
      meta: Option[((Int, String), Array[Byte])]): Future[String] = {
    val bucket = settings.snapshotsBucket

    if (log.isDebugEnabled) {
      log.debug(
        s"Saving snapshot to bucket [$bucket] for persistenceId [$persistenceId] (seqNr [$seqNr]) of size [${payload.length}] bytes")
    }

    val baseMeta = Map(
      "akka-entity-seq-nr" -> seqNr.toString,
      "akka-write-timestamp" -> writeTimestamp.toString,
      "akka-event-timestamp" -> eventTimestamp.toString,
      "akka-ser-id" -> serId.toString,
      "akka-ser-manifest" -> serManifest.toString)

    val withAkkaMeta = meta.fold(baseMeta) { case ((serId, manifest), payload) =>
      baseMeta ++ Map(
        "akka-meta-ser-id" -> serId.toString,
        "akka-meta-ser-manifest" -> serManifest,
        "akka-meta-payload" -> Base62.encode(payload))
    }

    val sortedTags = tags.toSeq.sorted.iterator.zipWithIndex.map { case (tag, idx) =>
      s"akka-tag-$idx" -> tag
    }

    val req = PutObjectRequest.builder
      .bucket(bucket)
      .key(keyForPid(persistenceId))
      .metadata((withAkkaMeta ++ sortedTags).asJava)
      .contentType("application/octet-stream")
      .build()

    client
      .putObject(req, AsyncRequestBody.fromBytes(payload))
      .asScala
      .andThen {
        case Success(_) =>
          if (log.isDebugEnabled) {
            log.debug(
              s"Saved snapshot to bucket [$bucket] for persistenceId [$persistenceId] (seqNr [$seqNr])",
              bucket,
              persistenceId,
              seqNr)
          }

        case Failure(ex) =>
          log.error(
            "Failed to save snapshot to bucket [{}] for persistenceId [{}] (seqNr [{}])",
            bucket,
            persistenceId,
            seqNr,
            ex)
      }
      .map(_ => bucket)(ExecutionContext.parasitic)
  }

  def keyForPid(pid: String): String =
    PersistenceId.extractEntityType(pid) match {
      case "" =>
        // No entity type: use the whole persistence ID
        pid

      case entityType =>
        val entityId = PersistenceId.extractEntityId(pid)
        s"$entityType/$entityId"
    }

  private val settings = S3FallbackSettings(config)
  private val client: S3AsyncClient = S3ClientProvider(system).clientFor(settings, configLocation)

  private val warmer =
    if (settings.warming.enabled) {
      Some(
        system.systemActorOf[WarmerCommand](
          Behaviors.setup { ctx =>
            Behaviors.withTimers { timers =>
              new WarmerBehavior(
                ctx,
                timers,
                Set(settings.eventsBucket, settings.snapshotsBucket),
                headBucket _,
                Clock.systemUTC(),
                settings.warming.target,
                settings.warming.period)
            }
          },
          s"S3Fallback-Warmer-$configLocation",
          Props.empty))
    } else None

  log.info("Started S3 fallback store with settings: {}", settings)

  // also warms up the connection pool
  private val eventFallbackInitialFuture =
    if (settings.eventsBucket.nonEmpty)
      Future.unit
        .flatMap(_ => headBucket(settings.eventsBucket))
        .recoverWith { ex =>
          log.error("Failed to HEAD events bucket, but continuing on", ex)
          Future.unit
        }
        .andThen(_ => warmer.foreach(_ ! ExternalOp))
    else
      Future.failed(new UnsupportedOperationException("Event fallback not enabled, must set events bucket to enable"))

  private val snapshotFallbackInitialFuture =
    if (settings.snapshotsBucket.nonEmpty)
      Future.unit
        .flatMap(_ => headBucket(settings.snapshotsBucket))
        .recoverWith { ex =>
          log.error("Failed to HEAD snapshots bucket, but continuing on", ex)
          Future.unit
        }
        .andThen(_ => warmer.foreach(_ ! ExternalOp))
    else
      Future.failed(
        new UnsupportedOperationException("Snapshot fallback not enabled, must set snapshots bucket to enable"))

  final def eventsPerFolder: Int = 1000

  private def headBucket(bucket: String): Future[Unit] =
    client
      .headBucket(_.bucket(bucket))
      .asScala
      .map(ConstantFun.scalaAnyToUnit)(ExecutionContext.parasitic)
      .recoverWith { ex =>
        log.error(s"Failed to HEAD bucket [$bucket]", ex)
        Future.failed(ex)
      }
}

object S3Fallback {
  val log = LoggerFactory.getLogger(getClass)

  sealed trait WarmerCommand

  case object ExternalOp extends WarmerCommand
  case object Check extends WarmerCommand
  case object Decrement extends WarmerCommand
  case object Closing extends WarmerCommand
  case class HeadDone(ex: Option[Throwable]) extends WarmerCommand

  final class WarmerBehavior(
      ctx: ActorContext[WarmerCommand],
      timers: TimerScheduler[WarmerCommand],
      buckets: Set[String],
      headBucket: String => Future[Unit],
      clock: Clock,
      target: Int,
      period: FiniteDuration)
      extends AbstractBehavior[WarmerCommand](ctx) {
    val max = 2 * target

    var opCount: Int = 0
    var lastOp: Instant = Instant.EPOCH
    var headed: List[String] = Nil
    var notHeaded: List[String] = buckets.iterator.filter(_.nonEmpty).toList

    ctx.self ! Check

    def recordOp(): Unit = {
      timers.startSingleTimer(s"decrement-$opCount", Decrement, period)
      opCount += 1
      lastOp = clock.instant()
      scheduleCheck()
    }

    def scheduleCheck(): Unit = {
      timers.startSingleTimer(Check, Check, delay)
    }

    def delay: FiniteDuration = period / (max + 1 - opCount)

    def onMessage(msg: WarmerCommand): Behavior[WarmerCommand] =
      msg match {
        case Closing => Behaviors.stopped

        case ExternalOp =>
          if (opCount < max) recordOp()

          this

        case Check =>
          if (opCount < target) {
            val bucket = notHeaded.head
            headed = bucket :: headed

            if (notHeaded.tail.isEmpty) {
              notHeaded = headed.reverse
              headed = Nil
            } else {
              notHeaded = headed.tail
            }

            ctx.pipeToSelf(headBucket(bucket)) { result =>
              HeadDone(result.failed.toOption)
            }

            recordOp()
          } else {
            scheduleCheck()
          }

          this

        case Decrement =>
          if (opCount > 0) {
            opCount -= 1
          }

          val sinceLast = (clock.instant().toEpochMilli - lastOp.toEpochMilli).millis

          if (delay <= sinceLast) {
            // we're now overdue
            timers.cancel(Check)
            ctx.self ! Check
          }

          this

        case HeadDone(failureOpt) =>
          failureOpt.foreach { ex =>
            log.warn("Warming HEAD request failed", ex)
          }

          this
      }
  }
}
