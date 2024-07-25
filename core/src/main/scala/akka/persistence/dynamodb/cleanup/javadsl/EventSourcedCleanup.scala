/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.cleanup.javadsl

import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletionStage
import java.util.{ List => JList }

import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.persistence.dynamodb.cleanup.scaladsl

/**
 * Java API: Tool for deleting or setting expiration (time to live) of events and/or snapshots for a given list of
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
final class EventSourcedCleanup private (delegate: scaladsl.EventSourcedCleanup) {

  def this(systemProvider: ClassicActorSystemProvider, configPath: String) =
    this(new scaladsl.EventSourcedCleanup(systemProvider, configPath))

  def this(systemProvider: ClassicActorSystemProvider) =
    this(systemProvider, "akka.persistence.dynamodb.cleanup")

  /**
   * Delete all events before a sequenceNr for the given persistence id. Snapshots are not deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param toSequenceNr
   *   sequence nr (inclusive) to delete up to
   */
  def deleteEventsTo(persistenceId: String, toSequenceNr: Long): CompletionStage[Done] =
    delegate.deleteEventsTo(persistenceId, toSequenceNr).asJava

  /**
   * Delete all events related to one single `persistenceId`. Snapshots are not deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   */
  def deleteAllEvents(persistenceId: String, resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAllEvents(persistenceId, resetSequenceNumber).asJava

  /**
   * Delete all events related to the given list of `persistenceIds`. Snapshots are not deleted.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   * @param resetSequenceNumber
   *   whether the entities will start from zero again, or continue from highest sequence number
   */
  def deleteAllEvents(persistenceIds: JList[String], resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAllEvents(persistenceIds.asScala.toVector, resetSequenceNumber).asJava

  // TODO: Delete before timestamp operations.
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
  // def deleteEventsBefore(persistenceId: String, timestamp: Instant): CompletionStage[Done] =
  //   delegate.deleteEventsBefore(persistenceId, timestamp).asJava

  // TODO: Delete before timestamp operations.
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
  // def deleteEventsBefore(entityType: String, slice: Int, timestamp: Instant): CompletionStage[Done] =
  //   delegate.deleteEventsBefore(entityType, slice, timestamp).asJava

  /**
   * Delete snapshots related to one single `persistenceId`. Events are not deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   */
  def deleteSnapshot(persistenceId: String): CompletionStage[Done] =
    delegate.deleteSnapshot(persistenceId).asJava

  /**
   * Delete all snapshots related to the given list of `persistenceIds`. Events are not deleted.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   */
  def deleteSnapshots(persistenceIds: JList[String]): CompletionStage[Done] =
    delegate.deleteSnapshots(persistenceIds.asScala.toVector).asJava

  /**
   * Deletes all events for the given persistence id from before the snapshot. The snapshot is not deleted. The event
   * with the same sequence number as the remaining snapshot is deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   */
  def cleanupBeforeSnapshot(persistenceId: String): CompletionStage[Done] =
    delegate.cleanupBeforeSnapshot(persistenceId).asJava

  /**
   * See single persistenceId overload for what is done for each persistence id.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   */
  def cleanupBeforeSnapshot(persistenceIds: JList[String]): CompletionStage[Done] =
    delegate.cleanupBeforeSnapshot(persistenceIds.asScala.toVector).asJava

  /**
   * Delete everything related to one single `persistenceId`. All events and snapshots are deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   */
  def deleteAll(persistenceId: String, resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAll(persistenceId, resetSequenceNumber).asJava

  /**
   * Delete everything related to the given list of `persistenceIds`. All events and snapshots are deleted.
   *
   * @param persistenceIds
   *   the persistence ids to delete for
   * @param resetSequenceNumber
   *   whether the entity will start from zero again, or continue from highest sequence number
   */
  def deleteAll(persistenceIds: JList[String], resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAll(persistenceIds.asScala.toVector, resetSequenceNumber).asJava

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
  def setExpiryForEvents(persistenceId: String, toSequenceNr: Long, expiryTimestamp: Instant): CompletionStage[Done] =
    delegate.setExpiryForEvents(persistenceId, toSequenceNr, expiryTimestamp).asJava

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
  def setExpiryForEvents(persistenceId: String, toSequenceNr: Long, timeToLive: Duration): CompletionStage[Done] =
    delegate.setExpiryForEvents(persistenceId, toSequenceNr, timeToLive.toScala).asJava

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
      expiryTimestamp: Instant): CompletionStage[Done] =
    delegate.setExpiryForAllEvents(persistenceId, resetSequenceNumber, expiryTimestamp).asJava

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
      timeToLive: Duration): CompletionStage[Done] =
    delegate.setExpiryForAllEvents(persistenceId, resetSequenceNumber, timeToLive.toScala).asJava

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
      persistenceIds: JList[String],
      resetSequenceNumber: Boolean,
      expiryTimestamp: Instant): CompletionStage[Done] =
    delegate
      .setExpiryForAllEvents(persistenceIds.asScala.toVector, resetSequenceNumber, expiryTimestamp)
      .asJava

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
      persistenceIds: JList[String],
      resetSequenceNumber: Boolean,
      timeToLive: Duration): CompletionStage[Done] =
    delegate
      .setExpiryForAllEvents(persistenceIds.asScala.toVector, resetSequenceNumber, timeToLive.toScala)
      .asJava

  /**
   * Set expiry (time to live) for the snapshot related to a single persistence id. Events do not have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param expiryTimestamp
   *   expiration timestamp to set on the snapshot
   */
  def setExpiryForSnapshot(persistenceId: String, expiryTimestamp: Instant): CompletionStage[Done] =
    delegate.setExpiryForSnapshot(persistenceId, expiryTimestamp).asJava

  /**
   * Set expiry (time to live) for the snapshot related to a single persistence id. Events do not have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForSnapshot(persistenceId: String, timeToLive: Duration): CompletionStage[Done] =
    delegate.setExpiryForSnapshot(persistenceId, timeToLive.toScala).asJava

  /**
   * Set expiry (time to live) for all snapshots related to the given list of persistence ids. Events do not have expiry
   * set.
   *
   * @param persistenceIds
   *   the persistence ids to set expiry for
   * @param expiryTimestamp
   *   expiration timestamp to set on all snapshots
   */
  def setExpiryForSnapshots(persistenceIds: JList[String], expiryTimestamp: Instant): CompletionStage[Done] =
    delegate.setExpiryForSnapshots(persistenceIds.asScala.toVector, expiryTimestamp).asJava

  /**
   * Set expiry (time to live) for all snapshots related to the given list of persistence ids. Events do not have expiry
   * set.
   *
   * @param persistenceIds
   *   the persistence ids to set expiry for
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForSnapshots(persistenceIds: JList[String], timeToLive: Duration): CompletionStage[Done] =
    delegate.setExpiryForSnapshots(persistenceIds.asScala.toVector, timeToLive.toScala).asJava

  /**
   * Set expiry (time to live) for all events for the given persistence id from before its snapshot. The snapshot does
   * not have expiry set. The event with the same sequence number as the snapshot does have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param expiryTimestamp
   *   expiration timestamp to set on all events before snapshot
   */
  def setExpiryForEventsBeforeSnapshot(persistenceId: String, expiryTimestamp: Instant): CompletionStage[Done] =
    delegate.setExpiryForEventsBeforeSnapshot(persistenceId, expiryTimestamp).asJava

  /**
   * Set expiry (time to live) for all events for the given persistence id from before its snapshot. The snapshot does
   * not have expiry set. The event with the same sequence number as the snapshot does have expiry set.
   *
   * @param persistenceId
   *   the persistence id to set expiry for
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForEventsBeforeSnapshot(persistenceId: String, timeToLive: Duration): CompletionStage[Done] =
    delegate.setExpiryForEventsBeforeSnapshot(persistenceId, timeToLive.toScala).asJava

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
      persistenceIds: JList[String],
      expiryTimestamp: Instant): CompletionStage[Done] =
    delegate.setExpiryForEventsBeforeSnapshots(persistenceIds.asScala.toVector, expiryTimestamp).asJava

  /**
   * Set expiry (time to live) for all events for the given persistence ids from before their snapshots. The snapshots
   * do not have expiry set. The events with the same sequence number as the snapshots do have expiry set.
   *
   * @param persistenceIds
   *   the persistence ids to set expiry for
   * @param timeToLive
   *   time-to-live duration from now
   */
  def setExpiryForEventsBeforeSnapshots(persistenceIds: JList[String], timeToLive: Duration): CompletionStage[Done] =
    delegate
      .setExpiryForEventsBeforeSnapshots(persistenceIds.asScala.toVector, timeToLive.toScala)
      .asJava

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
      expiryTimestamp: Instant): CompletionStage[Done] =
    delegate
      .setExpiryForAllEventsAndSnapshot(persistenceId, resetSequenceNumber, expiryTimestamp)
      .asJava

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
      timeToLive: Duration): CompletionStage[Done] =
    delegate
      .setExpiryForAllEventsAndSnapshot(persistenceId, resetSequenceNumber, timeToLive.toScala)
      .asJava

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
      persistenceIds: JList[String],
      resetSequenceNumber: Boolean,
      expiryTimestamp: Instant): CompletionStage[Done] =
    delegate
      .setExpiryForAllEventsAndSnapshots(persistenceIds.asScala.toVector, resetSequenceNumber, expiryTimestamp)
      .asJava

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
      persistenceIds: JList[String],
      resetSequenceNumber: Boolean,
      timeToLive: Duration): CompletionStage[Done] =
    delegate
      .setExpiryForAllEventsAndSnapshots(persistenceIds.asScala.toVector, resetSequenceNumber, timeToLive.toScala)
      .asJava

}
