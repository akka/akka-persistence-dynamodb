/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant

import akka.annotation.InternalApi

sealed trait ItemInSnapshotStore {
  def persistenceId: String
  def seqNr: Long

  def estimatedDynamoSize(entityType: String, ttlDefined: Boolean): Int = {
    import SnapshotAttributes._

    val base = itemFields +
      persistenceId.length +
      20 + // seqNr
      5 + entityType.length // entityType-slice

    if (ttlDefined) base + Expiry.length + 20 else base
  }
}

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
    extends BySliceQuery.SerializedItem
    with ItemInSnapshotStore {
  override def readTimestamp: Instant = eventTimestamp
  override def source: String = EnvelopeOrigin.SourceQuery

  override def estimatedDynamoSize(entityType: String, ttlDefined: Boolean): Int = {
    import SnapshotAttributes._
    import DynamoDBSizeCalculations._

    if (guaranteedTooManyBytes(payload)) 500000
    else {
      val baseSize = super.estimatedDynamoSize(entityType, ttlDefined)

      if (alreadyTooManyBytes(baseSize)) baseSize
      else {
        baseSize +
        WriteTimestamp.length + 20 +
        EventTimestamp.length + 20 +
        SnapshotSerId.length + 11 +
        SnapshotSerManifest.length + serManifest.length +
        SnapshotPayload.length + estimateByteArray(payload)
      }
    }
  }
}

final case class SerializedSnapshotMetadata(serId: Int, serManifest: String, payload: Array[Byte])

final case class SnapshotItemWithBreadcrumb(persistenceId: String, seqNr: Long, breadcrumb: (Int, String, Array[Byte]))
    extends ItemInSnapshotStore {

  def breadcrumbSerId = breadcrumb._1
  def breadcrumbSerManifest = breadcrumb._2
  def breadcrumbPayload = breadcrumb._3

  override def estimatedDynamoSize(entityType: String, ttlDefined: Boolean): Int = {
    import SnapshotAttributes._
    import DynamoDBSizeCalculations._

    if (guaranteedTooManyBytes(breadcrumbPayload)) 500000
    else {
      val baseSize = super.estimatedDynamoSize(entityType, ttlDefined)

      if (alreadyTooManyBytes(baseSize)) 500000 // I guess a ~400K persistenceId could happen...
      else {
        baseSize +
        BreadcrumbSerId.length + 11
        BreadcrumbSerManifest.length + breadcrumbSerManifest.length +
        BreadcrumbPayload.length + estimateByteArray(breadcrumbPayload)
      }
    }
  }
}

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
  val Expiry = "expiry"
  val BreadcrumbSerId = "breadcrumb_ser_id"
  val BreadcrumbSerManifest = "breadcrumb_ser_manifest"
  val BreadcrumbPayload = "breadcrumb_payload"

  private[internal] val itemFields =
    Pid.length + SeqNr.length + EntityTypeSlice.length
}
