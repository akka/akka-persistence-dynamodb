/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.journal

import java.time.Instant
import java.util.concurrent.CompletionException

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Try

import akka.Done
import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.AtomicWrite
import akka.persistence.PersistentRepr
import akka.persistence.SerializedEvent
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.dynamodb.internal.JournalDao
import akka.persistence.dynamodb.internal.MonotonicTimestamps
import akka.persistence.dynamodb.internal.PubSub
import akka.persistence.dynamodb.internal.SerializedEventMetadata
import akka.persistence.dynamodb.internal.SerializedJournalItem
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.journal.AsyncReplay
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.persistence.query.PersistenceQuery
import akka.persistence.typed.PersistenceId
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] object DynamoDBJournal {
  final case class WriteFinished(persistenceId: String, done: Future[_])

  def deserializeItem(serialization: Serialization, row: SerializedJournalItem): PersistentRepr = {
    if (row.payload.isEmpty)
      throw new IllegalStateException("Expected event payload to be loaded.")
    // note that FilteredPayload is not filtered out here, but that is handled by PersistentActor and EventSourcedBehavior
    val payload = serialization.deserialize(row.payload.get, row.serId, row.serManifest).get
    val repr = PersistentRepr(
      payload,
      row.seqNr,
      row.persistenceId,
      writerUuid = row.writerUuid,
      manifest = "", // classic event adapter not supported
      deleted = false,
      sender = ActorRef.noSender)

    val reprWithMeta = row.metadata match {
      case None => repr
      case Some(meta) =>
        repr.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    }
    reprWithMeta
  }

  val FutureDone: Future[Done] = Future.successful(Done)
}

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] final class DynamoDBJournal(config: Config, cfgPath: String)
    extends AsyncWriteJournal
    with AsyncReplay {
  import DynamoDBJournal._

  implicit val system: ActorSystem[_] = context.system.toTyped
  implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(context.system, classOf[DynamoDBJournal])

  private val sharedConfigPath = cfgPath.replaceAll("""\.journal$""", "")
  private val serialization: Serialization = SerializationExtension(context.system)
  private val settings = DynamoDBSettings(context.system.settings.config.getConfig(sharedConfigPath))
  log.debug("DynamoDB journal starting up")

  private val client = ClientProvider(system).clientFor(sharedConfigPath + ".client")
  private val journalDao = new JournalDao(system, settings, client)

  private val query = PersistenceQuery(system).readJournalFor[DynamoDBReadJournal](sharedConfigPath + ".query")

  private val pubSub: Option[PubSub] =
    if (settings.journalPublishEvents) Some(PubSub(system))
    else None

  private val monotonicTimestamps = MonotonicTimestamps(system)
  private val minTimestampFor: String => Option[Instant] = monotonicTimestamps.minTimestampFor(cfgPath)
  private val recordTimestampFor: (String, Instant) => Future[Done] = monotonicTimestamps.recordTimestampFor(cfgPath)

  // if there are pending writes when an actor restarts we must wait for
  // them to complete before we can read the highest sequence number or we will miss it
  private val writesInProgress = new java.util.HashMap[String, Future[Seq[Try[Unit]]]]()

  override def receivePluginInternal: Receive = { case WriteFinished(pid, f) =>
    writesInProgress.remove(pid, f)
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    def atomicWrite(atomicWrite: AtomicWrite): Future[Seq[Try[Unit]]] = {
      val serialized: Future[Seq[SerializedJournalItem]] =
        atomicWrite.payload
          .foldLeft(Future.successful(List.empty[SerializedJournalItem])) { (acc, pr) =>
            acc.flatMap { previousItems =>
              val (event, tags) = pr.payload match {
                case Tagged(payload, tags) =>
                  (payload.asInstanceOf[AnyRef], tags)
                case other =>
                  (other.asInstanceOf[AnyRef], Set.empty[String])
              }

              val serializedEvent = event match {
                case s: SerializedEvent => s // already serialized
                case _ =>
                  val bytes = serialization.serialize(event).get
                  val serializer = serialization.findSerializerFor(event)
                  val manifest = Serializers.manifestFor(serializer, event)
                  new SerializedEvent(bytes, serializer.identifier, manifest)
              }

              val metadata = pr.metadata.map { meta =>
                val m = meta.asInstanceOf[AnyRef]
                val serializedMeta = serialization.serialize(m).get
                val metaSerializer = serialization.findSerializerFor(m)
                val metaManifest = Serializers.manifestFor(metaSerializer, m)
                val id: Int = metaSerializer.identifier
                SerializedEventMetadata(id, metaManifest, serializedMeta)
              }

              // monotonically increasing, at least 1 microsecond more than previous timestamp
              val timestampFut = {
                val now = InstantFactory.now()
                minTimestampFor(pr.persistenceId)
                  .fold(Future.successful(now)) { min =>
                    if (min.isAfter(now)) {
                      log.warning(
                        "Detected possible clock skew: current timestamp [{}], required for monotonicity [{}]",
                        now,
                        min)
                      recordTimestampFor(pr.persistenceId, min).map(_ => min)(ExecutionContext.parasitic)
                    } else Future.successful(now)
                  }
              }

              timestampFut.map { timestamp =>
                SerializedJournalItem(
                  pr.persistenceId,
                  pr.sequenceNr,
                  timestamp,
                  InstantFactory.EmptyTimestamp,
                  Some(serializedEvent.bytes),
                  serializedEvent.serializerId,
                  serializedEvent.serializerManifest,
                  pr.writerUuid,
                  tags,
                  metadata) :: previousItems
              }(ExecutionContext.parasitic)
            }
          }
          .map(_.reverse)(ExecutionContext.parasitic)

      serialized.flatMap { writes =>
        journalDao
          .writeEvents(writes)
          .map { _ =>
            pubSub.foreach { ps =>
              atomicWrite.payload.zip(writes).foreach { case (pr, serialized) =>
                ps.publish(pr, serialized.writeTimestamp)
              }
            }
            Nil // successful writes
          }
          .recoverWith { case e: CompletionException =>
            e.getCause match {
              case error: ProvisionedThroughputExceededException => // reject retryable errors
                Future.successful(atomicWrite.payload.map(_ => Failure(error)))
              case error => // otherwise journal failure
                Future.failed(error)
            }
          }
      }
    }

    val persistenceId = messages.head.persistenceId
    val writeResult: Future[Seq[Try[Unit]]] =
      if (messages.size == 1)
        atomicWrite(messages.head)
      else {
        // persistAsync case
        // easiest to just group all into a single AtomicWrite
        val batch = AtomicWrite(messages.flatMap(_.payload))
        atomicWrite(batch)
      }

    writesInProgress.put(persistenceId, writeResult)
    writeResult.onComplete { _ =>
      self ! WriteFinished(persistenceId, writeResult)
    }
    writeResult
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val timeToLiveSettings = settings.timeToLiveSettings.eventSourcedEntities.get(entityType)
    timeToLiveSettings.useTimeToLiveForDeletes match {
      case Some(timeToLive) =>
        val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
        log.debug(
          "deleting events with time-to-live for persistence id [{}], to sequence number [{}], expiring at [{}]",
          persistenceId,
          toSequenceNr,
          expiryTimestamp)
        journalDao.updateEventExpiry(persistenceId, toSequenceNr, resetSequenceNumber = false, expiryTimestamp)
      case None =>
        log.debug("asyncDeleteMessagesTo persistenceId [{}], toSequenceNr [{}]", persistenceId, toSequenceNr)
        journalDao.deleteEventsTo(persistenceId, toSequenceNr, resetSequenceNumber = false)
    }
  }

  override def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Long] = {
    log.debug("replayMessages [{}] [{}]", persistenceId, fromSequenceNr)
    val pendingWrite = Option(writesInProgress.get(persistenceId)) match {
      case Some(f) =>
        log.debug("Write in progress for [{}], deferring replayMessages until write completed", persistenceId)
        // we only want to make write - replay sequential, not fail if previous write failed
        f.recover { case _ => Done }(ExecutionContext.parasitic)
      case None => FutureDone
    }
    pendingWrite.flatMap { _ =>
      if (toSequenceNr == Long.MaxValue && max == Long.MaxValue) {
        // this is the normal case, highest sequence number from last event
        val lastItem =
          query
            .internalCurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, includeDeleted = true)
            .runWith(Sink.fold(Option.empty[SerializedJournalItem]) { (_, item) =>
              // payload is empty for deleted item
              if (item.payload.isDefined) {
                val repr = deserializeItem(serialization, item)
                recoveryCallback(repr)
              }
              Some(item)
            })

        lastItem.flatMap { itemOpt =>
          itemOpt.fold(Future.successful[Long](0)) { item =>
            recordTimestampFor(item.persistenceId, item.eventTimestamp).map { _ =>
              item.seqNr
            }(ExecutionContext.parasitic)
          }
        }(ExecutionContext.parasitic)
      } else if (toSequenceNr <= 0) {
        // no replay
        journalDao.readHighestSequenceNr(persistenceId)
      } else {
        // replay to custom sequence number

        val highestSeqNr = journalDao.readHighestSequenceNr(persistenceId)

        val effectiveToSequenceNr =
          if (max == Long.MaxValue) toSequenceNr
          else math.min(toSequenceNr, fromSequenceNr + max - 1)

        query
          .internalCurrentEventsByPersistenceId(
            persistenceId,
            fromSequenceNr,
            effectiveToSequenceNr,
            includeDeleted = false)
          .runWith(Sink
            .foreach { item =>
              val repr = deserializeItem(serialization, item)
              recoveryCallback(repr)
            })
          .flatMap(_ => highestSeqNr)
      }
    }
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    throw new IllegalStateException(
      "asyncReplayMessages is not supposed to be called when implementing AsyncReplay. This is a bug, please report.")
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    throw new IllegalStateException(
      "asyncReplayMessages is not supposed to be called when implementing AsyncReplay. This is a bug, please report.")
  }

}
