/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant

import akka.annotation.InternalApi

final case class SerializedSnapshotItem(
    persistenceId: String,
    seqNr: Long,
    writeTimestamp: Instant,
    eventTimestamp: Instant,
    payload: Array[Byte],
    serId: Int,
    serManifest: String,
    tags: Set[String],
    metadata: Option[SerializedSnapshotMetadata])
    extends BySliceQuery.SerializedItem {
  override def readTimestamp: Instant = eventTimestamp
  override def source: String = EnvelopeOrigin.SourceQuery
}

final case class SerializedSnapshotMetadata(serId: Int, serManifest: String, payload: Array[Byte])

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SnapshotAttributes {
  // FIXME should attribute names be shorter?
  val Pid = "pid"
  val SeqNr = "seq_nr"
  val EntityTypeSlice = "entity_type_slice"
  val WriteTimestamp = "write_timestamp"
  val EventTimestamp = "event_timestamp"
  val SnapshotSerId = "snapshot_ser_id"
  val SnapshotSerManifest = "snapshot_ser_manifest"
  val SnapshotPayload = "snapshot_payload"
  val Tags = "tags"
  val MetaSerId = "meta_ser_id"
  val MetaSerManifest = "meta_ser_manifest"
  val MetaPayload = "meta_payload"
}
