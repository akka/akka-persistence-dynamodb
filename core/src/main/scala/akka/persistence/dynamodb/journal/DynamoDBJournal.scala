/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.journal

import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.Done
import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.AtomicWrite
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.SerializedEvent
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.dynamodb.internal.JournalDao
import akka.persistence.dynamodb.internal.PubSub
import akka.persistence.dynamodb.internal.SerializedEventMetadata
import akka.persistence.dynamodb.internal.SerializedJournalItem
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.journal.AsyncReplay
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.persistence.query.PersistenceQuery
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config

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

  private val persistenceExt = Persistence(system)

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

  // if there are pending writes when an actor restarts we must wait for
  // them to complete before we can read the highest sequence number or we will miss it
  private val writesInProgress = new java.util.HashMap[String, Future[_]]()

  override def receivePluginInternal: Receive = { case WriteFinished(pid, f) =>
    writesInProgress.remove(pid, f)
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    def atomicWrite(atomicWrite: AtomicWrite): Future[Done] = {
      val serialized: Try[Seq[SerializedJournalItem]] = Try {
        atomicWrite.payload.map { pr =>
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
          val timestamp = InstantFactory.now()

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
            metadata)
        }
      }

      serialized match {
        case Success(writes) =>
          journalDao
            .writeEvents(writes)
            .map { _ =>
              pubSub.foreach { ps =>
                atomicWrite.payload.zip(writes).foreach { case (pr, serialized) =>
                  ps.publish(pr, serialized.writeTimestamp)
                }
              }
              Done
            }

        case Failure(exc) =>
          Future.failed(exc)
      }
    }

    val persistenceId = messages.head.persistenceId
    val writeResult: Future[Done] =
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
    writeResult.map(_ => Nil)(ExecutionContexts.parasitic)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("asyncDeleteMessagesTo persistenceId [{}], toSequenceNr [{}]", persistenceId, toSequenceNr)
    journalDao.deleteEventsTo(persistenceId, toSequenceNr, resetSequenceNumber = false)
  }

  override def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Long] = {
    log.debug("replayMessages [{}] [{}]", persistenceId, fromSequenceNr)
    val pendingWrite = Option(writesInProgress.get(persistenceId)) match {
      case Some(f) =>
        log.debug("Write in progress for [{}], deferring replayMessages until write completed", persistenceId)
        // we only want to make write - replay sequential, not fail if previous write failed
        f.recover { case _ => Done }(ExecutionContexts.parasitic)
      case None => FutureDone
    }
    pendingWrite.flatMap { _ =>
      if (toSequenceNr == Long.MaxValue && max == Long.MaxValue) {
        // this is the normal case, highest sequence number from last event
        val seqNr = new AtomicLong
        query
          .internalCurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, includeDeleted = true)
          .runWith(Sink.foreach { item =>
            seqNr.set(item.seqNr)
            // payload is empty for deleted item
            if (item.payload.isDefined) {
              val repr = deserializeItem(serialization, item)
              recoveryCallback(repr)
            }
          })
          .map(_ => seqNr.get)(ExecutionContexts.parasitic)
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
