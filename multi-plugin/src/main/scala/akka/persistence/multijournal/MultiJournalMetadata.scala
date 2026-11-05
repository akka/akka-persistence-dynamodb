/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.util.NoSuchElementException

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest

/**
 * INTERNAL API
 *
 * Saved with events written to the "hot" journal, contains the highest seqNr written for the pid in the "cold" journals
 *
 * Constructor is package private for testing
 */
@InternalApi
private[multijournal] final class MultiJournalMetadata private[multijournal] (_bytes: Array[Byte])
    extends PartialFunction[Int, Long]
    with Iterable[(Int, Long)] {
  require((_bytes.length % 12) == 0, "valid metadata is a multiple of 12 bytes")
  import MultiJournalMetadata._

  override def size: Int = undefAbove - undefThrough
  override def knownSize: Int = size
  override def isEmpty: Boolean = undefAbove == undefThrough

  def isDefinedAt(journalId: Int): Boolean = (journalId > undefThrough) && (journalId <= undefAbove)
  def apply(journalId: Int): Long =
    if (isDefinedAt(journalId)) unsafeApply(journalId)
    else throw new NoSuchElementException(s"No value for journalId $journalId")

  override def applyOrElse[A1 <: Int, B1 >: Long](x: A1, default: A1 => B1): B1 =
    if (isDefinedAt(x)) unsafeApply(x) else default(x)

  def get(journalId: Int): Option[Long] =
    if (isDefinedAt(journalId)) Some(unsafeApply(journalId)) else None

  def iterator: Iterator[(Int, Long)] =
    if (undefThrough == Int.MaxValue) Iterator.empty
    else
      (undefThrough + 1 to undefAbove).iterator.map { journalId =>
        journalId -> unsafeApply(journalId)
      }

  def maxSeqNrKnown: Option[Long] = if (isEmpty) None else Some(unsafeApply(undefAbove))

  override def toMap[K, V](implicit ev: (Int, Long) <:< (K, V)): Map[K, V] = iterator.toMap
  override def toString: String = {
    val baseIterator = iterator
    new Iterator[String] {
      var nextToEmit: (Int, Long) = null
      var lastEmitted: (Int, Long) = null

      override def hasNext: Boolean =
        (nextToEmit ne null) || {
          if (baseIterator.hasNext) {
            val pulled = baseIterator.next()
            if ((lastEmitted eq null) || lastEmitted._2 != pulled._2) {
              nextToEmit = pulled
              true
            } else hasNext
          } else {
            lastEmitted = null
            false
          }
        }

      override def next(): String =
        if (!hasNext) throw new NoSuchElementException("no next element")
        else {
          lastEmitted = nextToEmit
          nextToEmit = null
          val (k, v) = lastEmitted
          s"$k -> $v"
        }
    }.mkString("MultiJournalMetadata(", ", ", ")")
  }

  def updated(journalId: Int, seqNr: Long): MultiJournalMetadata = {
    require(journalId >= 0, "journal IDs must be non-negative")
    require(seqNr > 0, "seqNrs must be positive")

    if (isDefinedAt(journalId)) {
      if (unsafeApply(journalId) == seqNr) this
      else {
        require(
          get(journalId - 1).forall(_ <= seqNr) && get(journalId + 1).forall(_ >= seqNr),
          s"Provided $seqNr for journal $journalId would break monotonicity: [${Seq(get(journalId - 1), Some(seqNr), get(journalId + 1)).flatten
            .mkString(", ")}]")
        val copy = Array.copyOf(bytes, bytes.length)
        val offset = unsafeOffsetFor(journalId) + 4
        (0 until 8).foldLeft(seqNr) { (acc, i) =>
          copy(offset + i) = ((acc >> 56) & 0xff).toByte
          acc << 8
        }
        new MultiJournalMetadata(copy)
      }
    } else if (isEmpty) {
      new MultiJournalMetadata(mapToBytes(Map(journalId -> seqNr), journalId, journalId))
    } else if (journalId <= undefThrough) {
      require(
        get(undefThrough + 1).forall(_ >= seqNr),
        s"Provided $seqNr for journal $journalId would break monotonicity: [$seqNr, ${unsafeApply(undefThrough + 1)}]")
      val n = undefThrough - journalId
      val newPrefix = new Array[Byte](12 * (1 + n))
      (0 to n).foreach { i =>
        val off = 12 * i
        val j = journalId + i
        newPrefix(off) = ((j >> 24) & 0xff).toByte
        newPrefix(off + 1) = ((j >> 16) & 0xff).toByte
        newPrefix(off + 2) = ((j >> 8) & 0xff).toByte
        newPrefix(off + 3) = (j & 0xff).toByte
        newPrefix(off + 4) = ((seqNr >> 56) & 0xff).toByte
        newPrefix(off + 5) = ((seqNr >> 48) & 0xff).toByte
        newPrefix(off + 6) = ((seqNr >> 40) & 0xff).toByte
        newPrefix(off + 7) = ((seqNr >> 32) & 0xff).toByte
        newPrefix(off + 8) = ((seqNr >> 24) & 0xff).toByte
        newPrefix(off + 9) = ((seqNr >> 16) & 0xff).toByte
        newPrefix(off + 10) = ((seqNr >> 8) & 0xff).toByte
        newPrefix(off + 11) = (seqNr & 0xff).toByte
      }
      new MultiJournalMetadata(Array.concat(newPrefix, bytes))
    } else {
      require(
        get(undefAbove).forall(_ <= seqNr),
        s"Provided $seqNr for journal $journalId would break monotonicity: [${unsafeApply(undefAbove)}, $seqNr]")
      val n = journalId - undefAbove
      val newSuffix = new Array[Byte](12 * n)
      val lowerSeqNr = unsafeApply(undefAbove)
      (0 until n).foreach { i =>
        val off = 12 * i
        val j = undefAbove + 1 + i
        val s = if (i == (n - 1)) seqNr else lowerSeqNr
        newSuffix(off) = ((j >> 24) & 0xff).toByte
        newSuffix(off + 1) = ((j >> 16) & 0xff).toByte
        newSuffix(off + 2) = ((j >> 8) & 0xff).toByte
        newSuffix(off + 3) = (j & 0xff).toByte
        newSuffix(off + 4) = ((s >> 56) & 0xff).toByte
        newSuffix(off + 5) = ((s >> 48) & 0xff).toByte
        newSuffix(off + 6) = ((s >> 40) & 0xff).toByte
        newSuffix(off + 7) = ((s >> 32) & 0xff).toByte
        newSuffix(off + 8) = ((s >> 24) & 0xff).toByte
        newSuffix(off + 9) = ((s >> 16) & 0xff).toByte
        newSuffix(off + 10) = ((s >> 8) & 0xff).toByte
        newSuffix(off + 11) = (s & 0xff).toByte
      }
      new MultiJournalMetadata(Array.concat(bytes, newSuffix))
    }
  }

  def bounds: (Int, Int) = undefThrough -> undefAbove

  def locateSequenceNr(seqNr: Long): Option[Int] = {
    if (definedEntries == 0) None
    else if (seqNr == Long.MaxValue || seqNr == 0) None
    else {
      val high = unsafeApply(undefAbove)
      if (high < seqNr) None
      else {
        val low = unsafeApply(undefThrough + 1)
        if (low >= seqNr) Some(undefThrough + 1)
        else {
          @annotation.tailrec
          def narrow(lowerExclusive: Int, upperInclusive: Int): Int = {
            val diff = upperInclusive - lowerExclusive
            if (diff == 1) upperInclusive
            else {
              val pivot = lowerExclusive + (diff / 2)
              unsafeApply(pivot).compareTo(seqNr) match {
                case negative if negative < 0 => narrow(pivot, upperInclusive)
                case _                        =>
                  // covers the exact match case, because an intermediate journal
                  // with no results would have its predecessor value, so we can't bail early
                  narrow(lowerExclusive, pivot)
              }
            }
          }
          Some(narrow(undefThrough + 1, undefAbove))
        }
      }
    }
  }

  override def equals(obj: Any): Boolean =
    obj match {
      case same: AnyRef if same eq this => true
      case other: MultiJournalMetadata =>
        val otherBounds = other.bounds
        val thisBounds = this.bounds
        (otherBounds == thisBounds) && {
          @annotation.tailrec
          def iter(cursor: Int): Boolean = {
            val same = other.unsafeApply(cursor) == this.unsafeApply(cursor)
            same && ((cursor == undefAbove) || iter(cursor + 1))
          }

          (otherBounds._1 == Int.MaxValue) || iter(undefThrough + 1)
        }

      case _ => false
    }

  override def hashCode: Int =
    _hashCode match {
      case 0 if !_hashed =>
        // not yet computed, or computation not yet visible
        // (OK to compute twice: underlying bytes never modified, so always set to same value
        val ret = hash(0, 0)
        _hashed = true
        ret

      case calculated => calculated
    }

  // only intended to be called by serializer
  private[multijournal] def unsafeToBytes: Array[Byte] = bytes

  private def unsafeApply(journalId: Int): Long = {
    val offset = unsafeOffsetFor(journalId) + 4
    (0 until 8).foldLeft(0L) { (acc, i) =>
      val read = bytes(offset + i).toLong & 0xff
      (acc << 8) + read
    }
  }

  /** Compute offset for the journalId key in the array */
  private def unsafeOffsetFor(journalId: Int): Int = {
    (journalId - (undefThrough + 1)) * 12
  }

  @annotation.tailrec
  private def hash(cursor: Int, acc: Int): Int =
    if (cursor >= bytes.length) {
      _hashCode = acc
      _hashCode
    } else {
      val i = (0 until 4).foldLeft(0) { (a, i) =>
        val read = bytes(cursor + i).toInt & 0xff
        (a << 8) + read
      }
      hash(cursor + 4, acc ^ i)
    }

  private val (undefThrough, bytes) = sortedBytes(_bytes)
  private val definedEntries = bytes.length / 12
  private val undefAbove = undefThrough + definedEntries
  private var _hashed: Boolean = false
  private var _hashCode: Int = 0
}

/** INTERNAL API */
@InternalApi
private[multijournal] object MultiJournalMetadata {

  final class Serializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest with BaseSerializer {
    val Manifest = "MultiJournalMetadata"

    def manifest(o: AnyRef): String =
      o match {
        case _: MultiJournalMetadata => Manifest
        case _                       => throw new IllegalArgumentException(s"misregistered class ${o.getClass.getName}")
      }

    def toBinary(o: AnyRef): Array[Byte] =
      o match {
        case mjm: MultiJournalMetadata => mjm.unsafeToBytes
        case _ => throw new IllegalArgumentException(s"misregistered class ${o.getClass.getName}")
      }

    final def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
      manifest match {
        case Manifest =>
          (bytes.length % 12) match {
            case remainder if remainder != 0 =>
              throw new IllegalArgumentException(s"$Manifest objects must have length congruent to 12")

            case _ if bytes.length == 0 => MultiJournalMetadata.Empty

            case _ =>
              val is = new DataInputStream(new ByteArrayInputStream(bytes))
              val (inOrder, journalId, seqNr) = (0 until (bytes.length / 12)).foldLeft((true, Option.empty[Int], 0L)) {
                case (acc @ (inOrder, j, s), i) =>
                  if (inOrder) {
                    val journalId = is.readInt()
                    if (journalId < 0) throw new IllegalArgumentException(s"Negative journalId found: [$journalId]")
                    if (j.forall(_ == journalId - 1)) {
                      val seqNr = is.readLong()
                      if (seqNr >= s) (true, Some(journalId), seqNr)
                      else (false, j, s)
                    } else (false, j, s)
                  } else acc
              }

              if (inOrder) new MultiJournalMetadata(bytes)
              else
                throw new IllegalArgumentException(
                  s"Malformed bytes detected after journalId [$journalId], seqNr [$seqNr]")
          }

        case _ => throw new IllegalArgumentException(s"misregistered manifest $manifest")
      }
  }

  val MaxNumberOfEntries = Int.MaxValue / 12

  def apply(highSeqNrs: Map[Int, Long]): MultiJournalMetadata =
    if (highSeqNrs.nonEmpty) {
      val journalIdsInOrder = highSeqNrs.keys.toSeq.sorted
      require(journalIdsInOrder.head >= 0, "journal IDs must be non-negative")
      val (seqNrsInOrder, _) = journalIdsInOrder.foldLeft(true -> 0L) { case (acc @ (inOrder, seqNr), journal) =>
        if (inOrder) {
          val nextSeqNr = highSeqNrs(journal)
          val nextInOrder = inOrder && (nextSeqNr >= seqNr)
          nextInOrder -> (if (nextInOrder) nextSeqNr else seqNr)
        } else acc
      }
      require(seqNrsInOrder, "sequence numbers must be (non-strictly) increasing")

      new MultiJournalMetadata(mapToBytes(highSeqNrs, journalIdsInOrder.head, journalIdsInOrder.last))
    } else Empty

  val Empty = new MultiJournalMetadata(Array.emptyByteArray)

  /**
   * INTERNAL API
   *
   * exposed to package only for testing
   */
  @InternalApi
  private[multijournal] def sortedBytes(bytes: Array[Byte]): (Int, Array[Byte]) = {
    val inputEntries = bytes.length / 12
    if (inputEntries == 0) Int.MaxValue -> Array.emptyByteArray
    else {
      var min = Int.MaxValue
      var max = Int.MinValue
      var inOrder: Boolean = true
      val is = new DataInputStream(new ByteArrayInputStream(bytes))
      val asMap = (0 until inputEntries).foldLeft(Map.empty[Int, Long]) { (maxSeqNrByJournal, _) =>
        val journalId = is.readInt()
        val seqNr = is.readLong()
        min = min.min(journalId)
        val nextMax = max.max(journalId)
        if (nextMax == max) { inOrder = false }
        max = nextMax
        maxSeqNrByJournal.get(journalId) match {
          case Some(existing) if existing >= seqNr => maxSeqNrByJournal
          case _                                   => maxSeqNrByJournal.updated(journalId, seqNr)
        }
      }
      is.close()
      require(min >= 0, "journalIds must not be negative")
      val outputEntries = max - min + 1

      if (inOrder && outputEntries == inputEntries) (min - 1) -> bytes
      else (min - 1) -> mapToBytes(asMap, min, max)
    }
  }

  private def mapToBytes(map: Map[Int, Long], min: Int, max: Int): Array[Byte] = {
    val outputEntries = 1 + max - min
    require(
      outputEntries <= MaxNumberOfEntries && outputEntries >= 0,
      s"journalId range cannot exceed $MaxNumberOfEntries")
    val baos = new ByteArrayOutputStream(12 * outputEntries)
    val os = new DataOutputStream(baos)
    (min to max).foldLeft(0L) { (last, journalId) =>
      val out = map.get(journalId).getOrElse(last)
      os.writeInt(journalId)
      os.writeLong(out)
      out
    }
    os.close()
    baos.toByteArray
  }
}
