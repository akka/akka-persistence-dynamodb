/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.cleanup.scaladsl

import java.time.Instant

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.JournalDao
import akka.persistence.dynamodb.internal.SnapshotDao
import akka.persistence.dynamodb.util.ClientProvider
import org.slf4j.LoggerFactory

/**
 * Scala API: Tool for deleting or setting expiration (time to live) of events and/or snapshots for a given list of
 * `persistenceIds` without using persistent actors.
 *
 * When running an operation with `EventSourcedCleanup` that deletes all events for a persistence id, the actor with
 * that persistence id must not be running! If the actor is restarted it would in that case be recovered to the wrong
 * state since the stored events have been deleted. Delete events before snapshot can still be used while the actor is
 * running.
 *
 * If `resetSequenceNumber` is `true` then an entity created with the same `persistenceId` will start from 0. Otherwise
 * it will continue from the latest highest used sequence number.
 *
 * WARNING: reusing the same `persistenceId` after resetting the sequence number should be avoided, since it might be
 * confusing to reuse the same sequence number for new events.
 *
 * When a list of `persistenceIds` are given, they are deleted sequentially in the same order as the list. It's possible
 * to parallelize the deletes by running several cleanup operations at the same time, each operating on different sets
 * of `persistenceIds`.
 */
@ApiMayChange
final class EventSourcedCleanup(systemProvider: ClassicActorSystemProvider, configPath: String) {

  def this(systemProvider: ClassicActorSystemProvider) =
    this(systemProvider, "akka.persistence.dynamodb.cleanup")

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] implicit val system: ActorSystem[_] = {
    import akka.actor.typed.scaladsl.adapter._
    systemProvider.classicSystem.toTyped
  }

  import system.executionContext

  private val log = LoggerFactory.getLogger(classOf[EventSourcedCleanup])

  private val sharedConfigPath = configPath.replaceAll("""\.cleanup$""", "")
  private val settings = DynamoDBSettings(system.settings.config.getConfig(sharedConfigPath))

  private val client = ClientProvider(system).clientFor(sharedConfigPath + ".client")
  private val journalDao = new JournalDao(system, settings, client)
  private val snapshotDao = new SnapshotDao(system, settings, client)

  /**
   * Delete all events before a sequenceNr for the given persistence id. Snapshots are not deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param toSequenceNr
   *   sequence nr (inclusive) to delete up to
   */
  def deleteEventsTo(persistenceId: String, toSequenceNr: Long): Future[Done] = {
    log.debug("deleteEventsTo persistenceId [{}], toSequenceNr [{}]", persistenceId, toSequenceNr)
    journalDao.deleteEventsTo(persistenceId, toSequenceNr, resetSequenceNumber = false).map(_ => Done)
  }

  /**
   * Delete all events related to one single `persistenceId`. Snapshots are not deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   */
  def deleteAllEvents(persistenceId: String, resetSequenceNumber: Boolean): Future[Done] = {
    journalDao
      .deleteEventsTo(persistenceId, toSequenceNr = Long.MaxValue, resetSequenceNumber)
      .map(_ => Done)
  }

  /**
   * Delete all events related to the given list of `persistenceIds`. Snapshots are not deleted.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   * @param resetSequenceNumber
   *   whether the entities will start from zero again, or continue from highest sequence number
   */
  def deleteAllEvents(persistenceIds: immutable.Seq[String], resetSequenceNumber: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAllEvents", pid => deleteAllEvents(pid, resetSequenceNumber))
  }

  // TODO: Delete before timestamp operations.
  //       Will either be full scans across the event journal, or require some kind of specialised indexing
  //       to get the highest sequence number before a timestamp, and then call deleteEventsTo with batching.
  //
  // /**
  //  * Delete events before a timestamp for the given persistence id. Snapshots are not deleted.
  //  *
  //  * This can be useful for `DurableStateBehavior` with change events, where the events are only used for the
  //  * Projections and not for the recovery of the `DurableStateBehavior` state. The timestamp may correspond to the
  //  * offset timestamp of the Projections, if events are not needed after all Projections have processed them.
  //  *
  //  * Be aware of that if all events of a persistenceId are removed the sequence number will start from 1 again if an
  //  * `EventSourcedBehavior` with the same persistenceId is used again.
  //  *
  //  * @param persistenceId
  //  *   the persistence id to delete for
  //  * @param timestamp
  //  *   timestamp (exclusive) to delete up to
  //  */
  // def deleteEventsBefore(persistenceId: String, timestamp: Instant): Future[Done] = {
  //   log.debug("deleteEventsBefore persistenceId [{}], timestamp [{}]", persistenceId, timestamp)
  //   journalDao.deleteEventsBefore(persistenceId, timestamp).map(_ => Done)
  // }

  // TODO: Delete before timestamp operations.
  //       Will either be full scans across the event journal, or require some kind of specialised indexing
  //       to find persistence ids and highest sequence numbers before a timestamp, then call deleteEventsTo.
  //
  // /**
  //  * Delete events before a timestamp for the given entityType and slice. Snapshots are not deleted.
  //  *
  //  * This can be useful for `DurableStateBehavior` with change events, where the events are only used for the
  //  * Projections and not for the recovery of the `DurableStateBehavior` state. The timestamp may correspond to the
  //  * offset timestamp of the Projections, if events are not needed after all Projections have processed them.
  //  *
  //  * Be aware of that if all events of a persistenceId are removed the sequence number will start from 1 again if an
  //  * `EventSourcedBehavior` with the same persistenceId is used again.
  //  *
  //  * @param entityType
  //  *   the entity type to delete for
  //  * @param slice
  //  *   the slice to delete for
  //  * @param timestamp
  //  *   timestamp (exclusive) to delete up to
  //  */
  // def deleteEventsBefore(entityType: String, slice: Int, timestamp: Instant): Future[Done] = {
  //   log.debug("deleteEventsBefore [{}], slice [{}] timestamp [{}]", entityType, slice, timestamp)
  //   journalDao.deleteEventsBefore(entityType, slice, timestamp).map(_ => Done)
  // }

  /**
   * Delete snapshots related to one single `persistenceId`. Events are not deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   */
  def deleteSnapshot(persistenceId: String): Future[Done] = {
    snapshotDao
      .delete(persistenceId, SnapshotSelectionCriteria(maxSequenceNr = Long.MaxValue))
      .map(_ => Done)
  }

  /**
   * Delete all snapshots related to the given list of `persistenceIds`. Events are not deleted.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   */
  def deleteSnapshots(persistenceIds: immutable.Seq[String]): Future[Done] = {
    foreach(persistenceIds, "deleteSnapshots", pid => deleteSnapshot(pid))
  }

  /**
   * Deletes all events for the given persistence id from before the snapshot. The snapshot is not deleted. The event
   * with the same sequence number as the remaining snapshot is deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   */
  def cleanupBeforeSnapshot(persistenceId: String): Future[Done] = {
    snapshotDao.load(persistenceId, SnapshotSelectionCriteria.Latest).flatMap {
      case None => Future.successful(Done)
      case Some(snapshot) =>
        deleteEventsTo(persistenceId, snapshot.seqNr)
    }
  }

  /**
   * See single persistenceId overload for what is done for each persistence id.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   */
  def cleanupBeforeSnapshot(persistenceIds: immutable.Seq[String]): Future[Done] = {
    foreach(persistenceIds, "cleanupBeforeSnapshot", pid => cleanupBeforeSnapshot(pid))
  }

  /**
   * Delete everything related to one single `persistenceId`. All events and snapshots are deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   */
  def deleteAll(persistenceId: String, resetSequenceNumber: Boolean): Future[Done] = {
    for {
      _ <- deleteAllEvents(persistenceId, resetSequenceNumber)
      _ <- deleteSnapshot(persistenceId)
    } yield Done
  }

  /**
   * Delete everything related to the given list of `persistenceIds`. All events and snapshots are deleted.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   */
  def deleteAll(persistenceIds: immutable.Seq[String], resetSequenceNumber: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAll", pid => deleteAll(pid, resetSequenceNumber))
  }

  /**
   * Set expiry (time to live) for all events up to a sequence number for the given persistence id. Snapshots do not
   * have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param toSequenceNr
   *   sequence number (inclusive) to set expiry
   * @param expiryTimestamp
   *   expiration timestamp to set on selected events
   */
  def setExpiryForEvents(persistenceId: String, toSequenceNr: Long, expiryTimestamp: Instant): Future[Done] = {
    journalDao
      .updateEventExpiry(persistenceId, toSequenceNr, resetSequenceNumber = false, expiryTimestamp)
      .map(_ => Done)
  }

  /**
   * Set expiry (time to live) for all events up to a sequence number for the given persistence id. Snapshots do not
   * have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param toSequenceNr
   *   sequence number (inclusive) to set expiry
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForEvents(persistenceId: String, toSequenceNr: Long, timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForEvents(persistenceId, toSequenceNr, expiryTimestamp)
  }

  /**
   * Set expiry (time to live) for all events related to a single persistence id. Snapshots do not have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   * @param expiryTimestamp
   *   expiration timestamp to set on all events
   */
  def setExpiryForAllEvents(
      persistenceId: String,
      resetSequenceNumber: Boolean,
      expiryTimestamp: Instant): Future[Done] = {
    journalDao
      .updateEventExpiry(persistenceId, toSequenceNr = Long.MaxValue, resetSequenceNumber, expiryTimestamp)
      .map(_ => Done)
  }

  /**
   * Set expiry (time to live) for all events related to a single persistence id. Snapshots do not have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForAllEvents(
      persistenceId: String,
      resetSequenceNumber: Boolean,
      timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForAllEvents(persistenceId, resetSequenceNumber, expiryTimestamp)
  }

  /**
   * Set expiry (time to live) for all events related to the given list of `persistenceIds`. Snapshots do not have
   * expiry set.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   * @param resetSequenceNumber
   *   whether the entities will start from zero again, or continue from highest sequence number
   * @param expiryTimestamp
   *   expiration timestamp to set on all events
   */
  def setExpiryForAllEvents(
      persistenceIds: immutable.Seq[String],
      resetSequenceNumber: Boolean,
      expiryTimestamp: Instant): Future[Done] = {
    foreach(
      persistenceIds,
      "setExpiryForAllEvents",
      pid => setExpiryForAllEvents(pid, resetSequenceNumber, expiryTimestamp))
  }

  /**
   * Set expiry (time to live) for all events related to the given list of `persistenceIds`. Snapshots do not have
   * expiry set.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   * @param resetSequenceNumber
   *   whether the entities will start from zero again, or continue from highest sequence number
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForAllEvents(
      persistenceIds: immutable.Seq[String],
      resetSequenceNumber: Boolean,
      timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForAllEvents(persistenceIds, resetSequenceNumber, expiryTimestamp)
  }

  /**
   * Set expiry (time to live) for the snapshot related to a single persistence id. Events do not have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param expiryTimestamp
   *   expiration timestamp to set on the snapshot
   */
  def setExpiryForSnapshot(persistenceId: String, expiryTimestamp: Instant): Future[Done] = {
    snapshotDao
      .updateExpiry(persistenceId, SnapshotSelectionCriteria(maxSequenceNr = Long.MaxValue), expiryTimestamp)
      .map(_ => Done)
  }

  /**
   * Set expiry (time to live) for the snapshot related to a single persistence id. Events do not have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForSnapshot(persistenceId: String, timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForSnapshot(persistenceId, expiryTimestamp)
  }

  /**
   * Set expiry (time to live) for all snapshots related to the given list of persistence ids. Events do not have expiry
   * set.
   *
   * @param persistenceIds
   *   the persistence ids to set expiry for
   * @param expiryTimestamp
   *   expiration timestamp to set on all snapshots
   */
  def setExpiryForSnapshots(persistenceIds: immutable.Seq[String], expiryTimestamp: Instant): Future[Done] = {
    foreach(persistenceIds, "setExpiryForSnapshots", pid => setExpiryForSnapshot(pid, expiryTimestamp))
  }

  /**
   * Set expiry (time to live) for all snapshots related to the given list of persistence ids. Events do not have expiry
   * set.
   *
   * @param persistenceIds
   *   the persistence ids to set expiry for
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForSnapshots(persistenceIds: immutable.Seq[String], timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForSnapshots(persistenceIds, expiryTimestamp)
  }

  /**
   * Set expiry (time to live) for all events for the given persistence id from before its snapshot. The snapshot does
   * not have expiry set. The event with the same sequence number as the snapshot does have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param expiryTimestamp
   *   expiration timestamp to set on all events before snapshot
   */
  def setExpiryForEventsBeforeSnapshot(persistenceId: String, expiryTimestamp: Instant): Future[Done] = {
    snapshotDao.load(persistenceId, SnapshotSelectionCriteria.Latest).flatMap {
      case None           => Future.successful(Done)
      case Some(snapshot) => setExpiryForEvents(persistenceId, snapshot.seqNr, expiryTimestamp)
    }
  }

  /**
   * Set expiry (time to live) for all events for the given persistence id from before its snapshot. The snapshot does
   * not have expiry set. The event with the same sequence number as the snapshot does have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForEventsBeforeSnapshot(persistenceId: String, timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForEventsBeforeSnapshot(persistenceId, expiryTimestamp)
  }

  /**
   * Set expiry (time to live) for all events for the given persistence ids from before their snapshots. The snapshots
   * do not have expiry set. The events with the same sequence number as the snapshots do have expiry set.
   *
   * @param persistenceIds
   *   the persistence ids to set expiry for
   * @param expiryTimestamp
   *   expiration timestamp to set on all events before snapshot
   */
  def setExpiryForEventsBeforeSnapshots(
      persistenceIds: immutable.Seq[String],
      expiryTimestamp: Instant): Future[Done] = {
    foreach(
      persistenceIds,
      "setExpiryForEventsBeforeSnapshot",
      pid => setExpiryForEventsBeforeSnapshot(pid, expiryTimestamp))
  }

  /**
   * Set expiry (time to live) for all events for the given persistence ids from before their snapshots. The snapshots
   * do not have expiry set. The events with the same sequence number as the snapshots do have expiry set.
   *
   * @param persistenceIds
   *   the persistence ids to set expiry for
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForEventsBeforeSnapshots(
      persistenceIds: immutable.Seq[String],
      timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForEventsBeforeSnapshots(persistenceIds, expiryTimestamp)
  }

  /**
   * Set expiry for everything related to a single persistence id. All events and the snapshot have expiry set.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   * @param expiryTimestamp
   *   expiration timestamp to set on all events before snapshot
   */
  def setExpiryForAllEventsAndSnapshot(
      persistenceId: String,
      resetSequenceNumber: Boolean,
      expiryTimestamp: Instant): Future[Done] = {
    for {
      _ <- setExpiryForAllEvents(persistenceId, resetSequenceNumber, expiryTimestamp)
      _ <- setExpiryForSnapshot(persistenceId, expiryTimestamp)
    } yield Done
  }

  /**
   * Set expiry for everything related to a single persistence id. All events and the snapshot have expiry set.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForAllEventsAndSnapshot(
      persistenceId: String,
      resetSequenceNumber: Boolean,
      timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForAllEventsAndSnapshot(persistenceId, resetSequenceNumber, expiryTimestamp)
  }

  /**
   * Set expiry for everything related to the given persistence ids. All events and snapshots have expiry set.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   * @param expiryTimestamp
   *   expiration timestamp to set on all events before snapshot
   */
  def setExpiryForAllEventsAndSnapshots(
      persistenceIds: immutable.Seq[String],
      resetSequenceNumber: Boolean,
      expiryTimestamp: Instant): Future[Done] = {
    foreach(
      persistenceIds,
      "setExpiryForAllEventsAndSnapshots",
      pid => setExpiryForAllEventsAndSnapshot(pid, resetSequenceNumber, expiryTimestamp))
  }

  /**
   * Set expiry for everything related to the given persistence ids. All events and snapshots have expiry set.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForAllEventsAndSnapshots(
      persistenceIds: immutable.Seq[String],
      resetSequenceNumber: Boolean,
      timeToLive: FiniteDuration): Future[Done] = {
    val expiryTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds)
    setExpiryForAllEventsAndSnapshots(persistenceIds, resetSequenceNumber, expiryTimestamp)
  }

  private def foreach(
      persistenceIds: immutable.Seq[String],
      operationName: String,
      pidOperation: String => Future[Done]): Future[Done] = {
    val size = persistenceIds.size
    log.info("Cleanup started {} of [{}] persistenceId.", operationName, size)

    def loop(remaining: List[String], n: Int): Future[Done] = {
      remaining match {
        case Nil => Future.successful(Done)
        case pid :: tail =>
          pidOperation(pid).flatMap { _ =>
            if (n % settings.cleanupSettings.logProgressEvery == 0)
              log.infoN("Cleanup {} [{}] of [{}].", operationName, n, size)
            loop(tail, n + 1)
          }
      }
    }

    val result = loop(persistenceIds.toList, n = 1)

    result.onComplete {
      case Success(_) =>
        log.info2("Cleanup completed {} of [{}] persistenceId.", operationName, size)
      case Failure(e) =>
        log.error(s"Cleanup {$operationName} failed.", e)
    }

    result
  }

}
