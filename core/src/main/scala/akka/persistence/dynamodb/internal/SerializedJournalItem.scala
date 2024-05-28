/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant

import akka.annotation.InternalApi

final case class SerializedJournalItem(
    persistenceId: String,
    seqNr: Long,
    writeTimestamp: Instant,
    payload: Option[Array[Byte]],
    serId: Int,
    serManifest: String,
    writerUuid: String,
    tags: Set[String],
    metadata: Option[SerializedEventMetadata])

final case class SerializedEventMetadata(serId: Int, serManifest: String, payload: Array[Byte])

/**
 * INTERNAL API
 */
@InternalApi private[akka] object JournalAttributes {
  // FIXME should attribute names be shorter?
  val Pid = "pid"
  val SeqNr = "seq_nr"
  // needed for the bySlices GSI
  val EntityTypeSlice = "entity_type_slice"
  val Timestamp = "timestamp"
  val EventSerId = "event_ser_id"
  val EventSerManifest = "event_ser_manifest"
  val EventPayload = "event_payload"
  val Writer = "writer"
  val MetaSerId = "meta_ser_id"
  val MetaSerManifest = "meta_ser_manifest"
  val MetaPayload = "meta_payload"
  val Deleted = "del"
}
