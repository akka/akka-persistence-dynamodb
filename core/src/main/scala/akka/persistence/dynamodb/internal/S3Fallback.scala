/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.annotation.InternalApi
import akka.persistence.dynamodb.ClientSettings
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.MinioLocalSettings
import akka.persistence.typed.PersistenceId
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest
import akka.util.Base62
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.jdk.FutureConverters._

import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/** INTERNAL API */
@InternalApi
private[internal] trait S3Fallback {
  def loadEvent(persistenceId: String, seqNr: Long, bucket: String): Future[Option[S3Fallback.EventFromS3]]
  def saveEvent(item: SerializedJournalItem): Future[Done]
  def loadSnapshot(persistenceId: String, bucket: String): Future[Option[S3Fallback.SnapshotFromS3]]
  def saveSnapshot(item: SerializedSnapshotItem): Future[Done]
}

/** INTERNAL API */
@InternalApi
private[dynamodb] object S3Fallback {
  def apply(
      settings: DynamoDBSettings,
      clientSettings: ClientSettings,
      actorSystem: ActorSystem[_]): Option[S3Fallback] = {
    if (settings.s3FallbackSettings.enabled) {
      val dynamicAccess = actorSystem.dynamicAccess

      if (dynamicAccess.classIsOnClasspath("software.amazon.awssdk.services.s3.S3AsyncClient"))
        Some(new RealS3Fallback(settings, clientSettings, actorSystem))
      else Some(NoS3Fallback)
    } else None
  }

  // Unlike the event, we need the seqNr and writeTimestamp in case a later snapshot gets saved
  // without being breadcrumbed in DDB: the later snapshot will replace the older snapshot and
  // we will play events from the later snapshot (viz. the snapshot store cannot assume that
  // when it follows the breadcrumb it will get the snapshot that led to that breadcrumb being written)
  case class SnapshotFromS3(
      seqNr: Long,
      writeTimestamp: Instant,
      eventTimestamp: Instant,
      payload: Array[Byte],
      serId: Int,
      serManifest: String,
      tags: Set[String],
      metadata: Option[SerializedSnapshotMetadata])

  // The event lacks the seqNr and timestamp because those are stored with the breadcrumb in DDB:
  // since the read journal asks for an event for a particular persistenceId and seqNr it can rely
  // on those
  case class EventFromS3(payload: Array[Byte], serId: Int, serManifest: String)
}

/** INTERNAL API */
@InternalApi
private[internal] object NoS3Fallback extends S3Fallback {
  def loadEvent(persistenceId: String, seqNr: Long, bucket: String): Future[Option[S3Fallback.EventFromS3]] =
    noS3ClientOnClasspath
  def saveEvent(item: SerializedJournalItem): Future[Done] = noS3ClientOnClasspath
  def loadSnapshot(persistenceId: String, bucket: String): Future[Option[S3Fallback.SnapshotFromS3]] =
    noS3ClientOnClasspath
  def saveSnapshot(item: SerializedSnapshotItem): Future[Done] = noS3ClientOnClasspath

  val noS3ClientOnClasspath = Future.failed(new IllegalStateException("S3 Client not on classpath"))
}

/** INTERNAL API */
@InternalApi
private[internal] class RealS3Fallback(
    ddbSettings: DynamoDBSettings,
    clientSettings: ClientSettings,
    system: ActorSystem[_])
    extends S3Fallback {
  def loadEvent(pid: String, seqNr: Long, bucket: String): Future[Option[S3Fallback.EventFromS3]] = {
    val eventFolder = seqNr / eventsPerFolder
    val key = s"${keyForPid(pid)}/$eventFolder/$seqNr"

    val req = GetObjectRequest.builder.bucket(bucket).key(key).build()

    client
      .getObject(req, AsyncResponseTransformer.toBytes[GetObjectResponse])
      .asScala
      .flatMap { respBytes =>
        val response = respBytes.response
        // AWS SDK constructs the byte array and promises not to modify
        val bytes = respBytes.asByteArrayUnsafe

        val metadata = response.metadata.asScala

        val serId = metadata.get("akkaSerId")
        val serManifest = metadata.get("akkaSerManifest")

        val attrs = Iterator(serId, serManifest).flatten.toIndexedSeq

        if (attrs.size == 2) {
          Future.successful(Some(S3Fallback.EventFromS3(bytes, attrs(0).toInt, attrs(1))))
        } else {
          Future.failed(new IllegalStateException("No Akka event metadata found"))
        }
      }(system.executionContext)
      .recover { case _: NoSuchKeyException | _: NoSuchBucketException =>
        None
      }(ExecutionContext.parasitic)
  }

  def saveEvent(item: SerializedJournalItem): Future[Done] = {
    item.payload match {
      case Some(payload) =>
        val eventFolder = item.seqNr / eventsPerFolder
        val key = s"${keyForPid(item.persistenceId)}/$eventFolder/${item.seqNr}"

        val req = PutObjectRequest.builder
          .bucket(settings.eventsBucket)
          .key(key)
          .metadata(Map(
            "akkaTimestamp" -> item.writeTimestamp.toString, // really here for observation/debugging/analysis
            "akkaSerId" -> item.serId.toString,
            "akkaSerManifest" -> item.serManifest,
            "akkaWriterUuid" -> item.writerUuid).asJava) // really here for observation/debugging/analysis
          .contentType("application/octet-stream")
          .build()

        client.putObject(req, AsyncRequestBody.fromBytes(payload)).asScala.map(_ => Done)(ExecutionContext.parasitic)

      case None =>
        Future.successful(Done)
    }
  }

  def loadSnapshot(pid: String, bucket: String): Future[Option[S3Fallback.SnapshotFromS3]] = {
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

        val seqNr = metadata.get("akkaEntitySeqNr")
        val writeTimestamp = metadata.get("akkaWriteTimestamp")
        val serId = metadata.get("akkaSerId")
        val serManifest = metadata.get("akkaSerManifest")
        val eventTimestamp = metadata.get("akkaEventTimestamp")

        val baseAttrs = Iterator(seqNr, writeTimestamp, serId, serManifest, eventTimestamp).flatten.toIndexedSeq

        if (baseAttrs.size == 5) {
          val metaSerId = metadata.get("akkaMetaSerId")
          val metaSerManifest = metadata.get("akkaMetaSerManifest")
          val metaPayload = metadata.get("akkaMetaPayload")

          val metaAttrs = Iterator(metaSerId, metaSerManifest, metaPayload).flatten.toIndexedSeq

          if (metaAttrs.isEmpty || metaAttrs.size == 3) {
            val tags = metadata.iterator.flatMap { case (k, v) =>
              if (k.startsWith("akkaTag-") && k.charAt(8).isDigit) Some(v)
              else None
            }.toSet

            Future.successful(Some(S3Fallback.SnapshotFromS3(
              seqNr = baseAttrs(0).toLong,
              writeTimestamp = Instant.parse(baseAttrs(1)),
              eventTimestamp = Instant.parse(baseAttrs(4)),
              payload = bytes,
              serId = baseAttrs(2).toInt,
              serManifest = baseAttrs(3),
              tags = tags,
              metadata =
                if (metaAttrs.isEmpty) None
                else
                  Some(SerializedSnapshotMetadata(
                    serId = metaAttrs(0).toInt,
                    serManifest = metaAttrs(1),
                    payload = Base62.decode(metaAttrs(2)))))))
          } else Future.failed(new IllegalStateException("Incomplete Akka snapshot metadata found"))
        } else {
          Future.failed(new IllegalStateException("No Akka snapshot metadata found"))
        }
      }(system.executionContext)
      .recover { case _: NoSuchKeyException | _: NoSuchBucketException =>
        None
      }(ExecutionContext.parasitic)
  }

  def saveSnapshot(item: SerializedSnapshotItem): Future[Done] = {
    val req = PutObjectRequest.builder
      .bucket(settings.snapshotsBucket)
      .key(keyForPid(item.persistenceId))
      .metadata(s3MetadataFor(item).asJava)
      .contentType("application/octet-stream")
      .build()

    client
      .putObject(req, AsyncRequestBody.fromBytes(item.payload))
      .asScala
      .map(_ => Done)(ExecutionContext.parasitic)
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

  def s3MetadataFor(item: SerializedSnapshotItem): Map[String, String] = {
    val baseMap = Map(
      "akkaEntitySeqNr" -> item.seqNr.toString,
      "akkaWriteTimestamp" -> item.writeTimestamp.toString,
      "akkaEventTimestamp" -> item.eventTimestamp.toString,
      "akkaSerId" -> item.serId.toString,
      "akkaSerManifest" -> item.serManifest.toString)

    val withAkkaMeta = item.metadata.fold(baseMap) { m =>
      baseMap ++ Map(
        "akkaMetaSerId" -> m.serId.toString,
        "akkaMetaSerManifest" -> m.serManifest,
        "akkaMetaPayload" -> Base62.encode(m.payload))
    }

    val sortedTags = item.tags.toSeq.sorted.iterator.zipWithIndex.map { case (tag, idx) =>
      s"akkaTag-$idx" -> tag
    }

    withAkkaMeta ++ sortedTags
  }

  def settings = ddbSettings.s3FallbackSettings

  private val clientSpec = S3ClientProvider.s3ClientSpec(ddbSettings, clientSettings)

  private def client = S3ClientProvider(system).clientFor(clientSpec)

  final def eventsPerFolder: Int = 1000
}

/** INTERNAL API */
@InternalApi
object S3ClientProvider extends ExtensionId[S3ClientProvider] {
  def createExtension(system: ActorSystem[_]): S3ClientProvider = new S3ClientProvider(system)

  def get(system: ActorSystem[_]): S3ClientProvider = apply(system)

  private[internal] final case class S3ClientSpec(
      region: Option[String],
      minioLocalSettings: MinioLocalSettings,
      maxConcurrency: Int,
      maxPendingConnectionAcquires: Int,
      readTimeout: FiniteDuration,
      writeTimeout: FiniteDuration,
      connectionTimeout: FiniteDuration,
      connectionAcquisitionTimeout: FiniteDuration,
      connectionTimeToLive: FiniteDuration,
      useIdleConnectionReaper: Boolean,
      connectionMaxIdleTime: FiniteDuration,
      tlsNegotiationTimeout: FiniteDuration,
      tcpKeepAlive: Boolean)

  private[internal] def s3ClientSpec(ddbSettings: DynamoDBSettings, clientSettings: ClientSettings): S3ClientSpec =
    S3ClientSpec(
      clientSettings.region,
      ddbSettings.s3FallbackSettings.minioLocal,
      clientSettings.http.maxConcurrency,
      clientSettings.http.maxPendingConnectionAcquires,
      clientSettings.http.readTimeout,
      clientSettings.http.writeTimeout,
      clientSettings.http.connectionTimeout,
      clientSettings.http.connectionAcquisitionTimeout,
      clientSettings.http.connectionTimeToLive,
      clientSettings.http.useIdleConnectionReaper,
      clientSettings.http.connectionMaxIdleTime,
      clientSettings.http.tlsNegotiationTimeout,
      clientSettings.http.tcpKeepAlive)
}

/** INTERNAL API */
@InternalApi
final class S3ClientProvider(system: ActorSystem[_]) extends Extension {
  import S3ClientProvider.S3ClientSpec

  // only the minio local settings and HTTP client settings affect this
  private val clients = new ConcurrentHashMap[S3ClientSpec, S3AsyncClient]

  CoordinatedShutdown(system)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close S3 clients") { () =>
      // FIXME: as with DDB clients, should this be run on a blocking dispatcher?
      clients.asScala.values.foreach(_.close())
      Future.successful(Done)
    }

  def clientFor(spec: S3ClientSpec): S3AsyncClient =
    clients.computeIfAbsent(spec, settings => createClient(settings))

  private def createClient(spec: S3ClientSpec): S3AsyncClient = {
    // At least for now, use the same http client settings as DDB
    val httpClientBuilder = NettyNioAsyncHttpClient.builder
      .maxConcurrency(spec.maxConcurrency)
      .maxPendingConnectionAcquires(spec.maxPendingConnectionAcquires)
      .readTimeout(spec.readTimeout.toJava)
      .writeTimeout(spec.writeTimeout.toJava)
      .connectionTimeout(spec.connectionTimeout.toJava)
      .connectionAcquisitionTimeout(spec.connectionAcquisitionTimeout.toJava)
      .connectionTimeToLive(spec.connectionTimeToLive.toJava)
      .useIdleConnectionReaper(spec.useIdleConnectionReaper)
      .connectionMaxIdleTime(spec.connectionMaxIdleTime.toJava)
      .tlsNegotiationTimeout(spec.tlsNegotiationTimeout.toJava)
      .tcpKeepAlive(spec.tcpKeepAlive)

    // throws if not on classpath
    val s3Clazz = system.dynamicAccess
      .getClassFor[S3AsyncClient]("software.amazon.awssdk.services.s3.S3AsyncClient")
      .get

    var builder = s3Clazz.getDeclaredMethod("builder").invoke(null).asInstanceOf[S3AsyncClientBuilder]

    builder = builder
      .httpClientBuilder(httpClientBuilder)

    // otherwise default region lookup
    spec.region.foreach { region =>
      builder = builder.region(Region.of(region))
    }

    if (spec.minioLocalSettings.enabled) {
      // Use Minio
      val localSettings = spec.minioLocalSettings

      builder = builder
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create(s"http://${localSettings.host}:${localSettings.port}"))
        .credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create(localSettings.accessKey, localSettings.secretKey)))
        .forcePathStyle(true)
    }

    builder.build()
  }
}

object S3FallbackSerializer {
  // OK to share this, even between ActorSystems in the same JVM
  private val cache = new ConcurrentHashMap[S3Breadcrumb, Array[Byte]]
  val BreadcrumbManifest = "akka.persistence.dynamodb.S3Breadcrumb"
  val SnapshotManifest = "akka.persistence.dynamodb.SnapshotInS3"
  val Utf8 = StandardCharsets.UTF_8
}

class S3FallbackSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  import S3FallbackSerializer._

  def identifier: Int = BaseSerializer.identifierFromConfig("s3-fallback", system)

  def manifest(o: AnyRef): String =
    o match {
      case _: S3Breadcrumb => BreadcrumbManifest
      case _               => throw new scala.MatchError(o)
    }

  def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case bc @ S3Breadcrumb(bucket) =>
        cache.get(bc) match {
          case bytes: Array[Byte] => bytes
          case null =>
            val bytes = bucket.getBytes(Utf8)
            // No concurrency control, just a cache (eventually this will converge)
            cache.putIfAbsent(bc, bytes)
            cache.get(bc)
        }

      case _ => throw new scala.MatchError(o)
    }

  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case BreadcrumbManifest =>
        S3Breadcrumb(new String(bytes, Utf8))

      case _ => throw new scala.MatchError(manifest)
    }
}

// The breadcrumb is what is written in place of the snapshot or event to DDB.  Since a snapshot is read by persistence ID and
// events by persistence ID and seqNr, the DDB query which retrieves the breadcrumb can fill in the remaining coordinates
/** INTERNAL API */
@InternalApi
case class S3Breadcrumb(bucket: String)
