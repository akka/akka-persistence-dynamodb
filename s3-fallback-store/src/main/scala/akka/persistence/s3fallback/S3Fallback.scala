/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.s3fallback

import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.internal.FallbackStore
import akka.persistence.typed.PersistenceId
import akka.util.Base62
import com.typesafe.config.Config
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.FutureConverters._

import java.time.Instant

final class S3Fallback(system: ActorSystem[_], config: Config, configLocation: String) extends FallbackStore[String] {
  import FallbackStore._

  override def toBreadcrumb(maybeBreadcrumb: AnyRef): Option[String] =
    maybeBreadcrumb match {
      case s: String => Some(s)
      case _         => None
    }

  override def breadcumbClass: Class[String] = classOf[String]

  override def loadEvent(
      bucket: String, // the breadcrumb
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[EventFromFallback]] = {
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
        }(system.executionContext)
        .recover { case _: NoSuchKeyException | _: NoSuchBucketException =>
          None
        }(ExecutionContext.parasitic)
    } else {
      client
        .headObject(HeadObjectRequest.builder.bucket(bucket).key(key).build)
        .asScala
        .flatMap { response =>
          event(None, response.metadata.asScala.toMap)
        }(system.executionContext)
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
      payload: Array[Byte]): Future[String] = {
    val eventFolder = seqNr / eventsPerFolder
    val key = s"${keyForPid(persistenceId)}/$eventFolder/$seqNr"

    val bucket = settings.eventsBucket

    val req = PutObjectRequest.builder
      .bucket(bucket)
      .key(key)
      .metadata(Map("akka-ser-id" -> serId.toString, "akka-ser-manifest" -> serManifest).asJava)
      .contentType("application/octet-stream")
      .build()

    client.putObject(req, AsyncRequestBody.fromBytes(payload)).asScala.map(_ => bucket)(ExecutionContext.parasitic)
  }

  def loadSnapshot(
      bucket: String, // aka breadcrumb
      pid: String,
      seqNr: Long): Future[Option[FallbackStore.SnapshotFromFallback]] = {
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
      }(system.executionContext)
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
      meta: Option[((Int, String), Array[Byte])]): Future[String] = {
    val bucket = settings.snapshotsBucket

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

  private val client = S3ClientProvider(system).clientFor(settings)

  final def eventsPerFolder: Int = 1000
}
