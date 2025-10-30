/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant

import akka.annotation.InternalApi

sealed trait ItemInJournal {
  def persistenceId: String
  def seqNr: Long
  def writeTimestamp: Instant
  def readTimestamp: Instant
  def writerUuid: String
  def tags: Set[String]
  def metadata: Option[SerializedEventMetadata]

  def estimatedDynamoSize(entityType: String, ttlDefined: Boolean): Int = {
    import JournalAttributes._
    import DynamoDBSizeCalculations._

    val base = itemFields +
      persistenceId.length +
      20 + // seqNr
      5 + entityType.length + // entityType-slice
      20 + // writeTimestamp
      writerUuid.length

    val withTags =
      if (tags.nonEmpty) tags.foldLeft(Tags.length + 3 + base) { (size, tag) => size + 3 + tag.length }
      else base

    val withMeta =
      metadata.fold(withTags) { meta =>
        if (guaranteedTooManyBytes(meta.payload)) 500000 // value too big for DynamoDB
        else {
          withTags +
          MetaSerId.length + 11 +
          MetaSerManifest.length + meta.serManifest.length +
          MetaPayload.length + estimateByteArray(meta.payload)
        }
      }

    if (ttlDefined) withMeta + 20 + Expiry.length else withMeta
  }
}

final case class SerializedJournalItem(
    persistenceId: String,
    seqNr: Long,
    writeTimestamp: Instant,
    readTimestamp: Instant,
    payload: Option[Array[Byte]],
    serId: Int,
    serManifest: String,
    writerUuid: String,
    tags: Set[String],
    metadata: Option[SerializedEventMetadata])
    extends BySliceQuery.SerializedItem
    with ItemInJournal {

  override def eventTimestamp: Instant = writeTimestamp

  override def source: String =
    if (payload.isDefined) EnvelopeOrigin.SourceQuery else EnvelopeOrigin.SourceBacktracking

  // This is an approximation of https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
  // * strings are assumed to be all-ASCII (viz. this will often underestimate the size of strings)
  // * numbers are assumed to use all digits possible (viz. 20 for a long, 11 for an Int: overestimate)
  // * byte arrays are assumed to use 1.36 bytes per byte (which is an overestimate: Base64 actually uses 4 bytes for every 3)
  //
  // in the expected case, most of the contribution to large writes will come from the byte arrays (especially relative to long
  // non-ASCII strings), so this is more likely to be an overestimate; if this expectation doesn't match reality, then it
  // can underestimate
  override def estimatedDynamoSize(entityType: String, ttlDefined: Boolean): Int = {
    import JournalAttributes._
    import DynamoDBSizeCalculations._

    if (payload.exists(guaranteedTooManyBytes)) 500000
    else {
      val baseSize = super.estimatedDynamoSize(entityType, ttlDefined)

      if (alreadyTooManyBytes(baseSize)) baseSize
      else {
        baseSize +
        EventSerId.length + 11 +
        EventSerManifest.length + serManifest.length +
        EventPayload.length + payload.fold(0)(estimateByteArray)
      }
    }
  }
}

final case class SerializedEventMetadata(serId: Int, serManifest: String, payload: Array[Byte])

final case class JournalItemWithBreadcrumb(
    persistenceId: String,
    seqNr: Long,
    writeTimestamp: Instant,
    readTimestamp: Instant,
    writerUuid: String,
    tags: Set[String],
    metadata: Option[SerializedEventMetadata],
    breadcrumbSerId: Int,
    breadcrumbSerManifest: String,
    breadcrumbPayload: Option[Array[Byte]])
    extends ItemInJournal {
  override def estimatedDynamoSize(entityType: String, ttlDefined: Boolean): Int = {
    import JournalAttributes._
    import DynamoDBSizeCalculations._

    if (breadcrumbPayload.exists(guaranteedTooManyBytes)) 500000
    else {
      val baseSize = super.estimatedDynamoSize(entityType, ttlDefined)

      if (alreadyTooManyBytes(baseSize)) baseSize
      else {
        baseSize +
        BreadcrumbSerId.length + 11 +
        BreadcrumbSerManifest.length + breadcrumbSerManifest.length +
        BreadcrumbPayload.length + breadcrumbPayload.fold(0)(estimateByteArray)
      }
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object JournalAttributes {
  // FIXME should attribute names be shorter?
  val Pid = "pid"
  val SeqNr = "seq_nr"
  // needed for the bySlices GSI
  val EntityTypeSlice = "entity_type_slice"
  val Timestamp = "ts"
  val EventSerId = "event_ser_id"
  val EventSerManifest = "event_ser_manifest"
  val EventPayload = "event_payload"
  val Writer = "writer"
  val Tags = "tags"
  val MetaSerId = "meta_ser_id"
  val MetaSerManifest = "meta_ser_manifest"
  val MetaPayload = "meta_payload"
  val Deleted = "del"
  val Expiry = "expiry"
  val ExpiryMarker = "expiry_marker"
  val BreadcrumbSerId = "breadcrumb_ser_id"
  val BreadcrumbSerManifest = "breadcrumb_ser_manifest"
  val BreadcrumbPayload = "breadcrumb_payload"

  // The lengths of the fields that get always get written
  private[internal] val itemFields =
    Pid.length + SeqNr.length + EntityTypeSlice.length + Timestamp.length + Writer.length
}

object DynamoDBSizeCalculations {
  def guaranteedTooManyBytes(bytes: Array[Byte]): Boolean =
    bytes.length >= 307200 // (4 * 307200) / 3 is greater than 400 * 1024
  def alreadyTooManyBytes(size: Int): Boolean =
    size >= 409600 // Assumes that the item size limit is in KiB, which is larger than KB

  def estimateByteArray(bytes: Array[Byte]): Int =
    if (guaranteedTooManyBytes(bytes)) 500000 // arbitrary "too big" value
    else (34 * bytes.length) / 25 // an overestimate
}
