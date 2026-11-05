/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

import scala.util.Random

import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import org.scalactic.Prettifier
import org.scalatest.OptionValues
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MultiJournalMetadataSpec
    extends AnyWordSpec
    with BeforeAndAfterAll
    with Matchers
    with OptionValues
    with ScalaFutures {

  implicit private val prettifier: Prettifier = Prettifier {
    case metadata: MultiJournalMetadata => metadata.toString
    case other                          => Prettifier.default(other)
  }

  val system = ActorSystem(getClass.getSimpleName).asInstanceOf[ExtendedActorSystem]
  val serializer = new MultiJournalMetadata.Serializer(system)

  override def afterAll(): Unit = {
    system.terminate().futureValue
    super.afterAll()
  }

  "Empty MultiJournalMetadata" should {
    "be empty" in {
      MultiJournalMetadata.Empty.size shouldBe 0
      MultiJournalMetadata.Empty.isEmpty shouldBe true
      (1 to 1000).foreach { _ =>
        val i = Random.nextInt()
        assert(!MultiJournalMetadata.Empty.isDefinedAt(i), s"isDefinedAt($i) was true")
        assert(!MultiJournalMetadata.Empty.isDefinedAt(-i), s"isDefinedAt(-$i) was true")
        withClue(s"journalId=$i") {
          MultiJournalMetadata.Empty.get(i) shouldBe empty
          MultiJournalMetadata.Empty.get(-i) shouldBe empty
        }
      }
      MultiJournalMetadata.Empty.toMap shouldBe empty
      MultiJournalMetadata.Empty.unsafeToBytes shouldBe empty
    }

    "report bounds" in {
      // rationale: lower, exclusive, bound is the greatest int such that all journalIds in the metadata are defined
      // upper, inclusive bound is the lower bound plus the number of entries in the metadata
      MultiJournalMetadata.Empty.bounds shouldBe (Int.MaxValue -> Int.MaxValue)
    }

    "not locate sequence numbers" in {
      (1 to 1000).foreach { _ =>
        val i = Random.nextLong(Long.MaxValue).max(1)
        withClue(s"seqNr=$i") {
          MultiJournalMetadata.Empty.locateSequenceNr(i) shouldBe empty
        }
      }
    }

    "be updatable" in {
      (1 to 1000).foreach { _ =>
        val i = Random.nextInt(2000)
        val seqNr = Random.nextLong(Long.MaxValue).max(1)
        withClue(s"journalId=$i seqNr=$seqNr") {
          val updated = MultiJournalMetadata.Empty.updated(i, seqNr)
          MultiJournalMetadata.Empty.get(i) shouldBe empty
          assert(updated.isDefinedAt(i), s"should have value for journalId=$i")
          updated.get(i) should contain(seqNr)
          updated.toMap should contain theSameElementsAs Map(i -> seqNr)
          updated.unsafeToBytes.length shouldBe 12

          updated.size shouldBe 1
          updated.bounds shouldBe ((i - 1) -> i)

          val alsoUpdated = MultiJournalMetadata.Empty.updated(i, seqNr)
          assert(alsoUpdated ne updated, s"should return a different object")
          assert(alsoUpdated == updated, s"should reutrn an equal object")
          alsoUpdated.hashCode shouldBe updated.hashCode
        }
      }
    }

    "be constructible from an empty Map" in {
      val constructed = MultiJournalMetadata(Map.empty)
      constructed shouldBe theSameInstanceAs(MultiJournalMetadata.Empty)
    }

    "serialize round-trip" in {
      val manifest = serializer.manifest(MultiJournalMetadata.Empty)
      val bytes = serializer.toBinary(MultiJournalMetadata.Empty)
      serializer.fromBinary(bytes, manifest) shouldBe MultiJournalMetadata.Empty
    }
  }

  "non-empty MultiJournalMetadata" should {
    "be constructible from a non-empty Map" in {
      val map = Map(1 -> 42L)
      val single = MultiJournalMetadata(map)
      assert(!single.isEmpty)
      single.size shouldBe 1
      single.bounds shouldBe (0 -> 1)
      single.get(1) should contain(42)
      single.toMap should contain theSameElementsAs map
      single.unsafeToBytes.size shouldBe 12
    }

    "fail to construct for negative journals" in {
      an[IllegalArgumentException] shouldBe thrownBy { MultiJournalMetadata(Map(-1 -> 500)) }
    }

    "normalize in its constructor" in {
      val single = MultiJournalMetadata(Map(17 -> 76, 18 -> 123))
      val singleBytes = single.unsafeToBytes
      val swapped = Array.copyOf(singleBytes, singleBytes.length)
      (0 until singleBytes.length).foreach { i =>
        swapped(i) = singleBytes((i + 12) % singleBytes.length)
      }
      val fromBytes = new MultiJournalMetadata(swapped)
      fromBytes shouldBe single
    }

    "validate monotonicity when building" in {
      val iae = the[IllegalArgumentException] thrownBy MultiJournalMetadata(Map(0 -> 14L, 1 -> 13L))
      iae.getMessage should include("must be (non-strictly) increasing")
    }

    "locate journal by sequence number" in {
      val metadata = MultiJournalMetadata(Map(0 -> 5, 2 -> 126))
      metadata.locateSequenceNr(Long.MaxValue) shouldBe None
      metadata.locateSequenceNr(0) shouldBe empty
      metadata.locateSequenceNr(1) should contain(0)
      metadata.locateSequenceNr(5) should contain(0)
      metadata.locateSequenceNr(6) should contain(2)
      metadata.get(1) should contain(5L)
      metadata.locateSequenceNr(126) should contain(2)
      metadata.locateSequenceNr(127) shouldBe empty
    }

    "be updatable" in {
      val singleBase = MultiJournalMetadata(Map(13 -> 56))
      an[IllegalArgumentException] shouldBe thrownBy { singleBase.updated(-7, 56) }
      an[IllegalArgumentException] shouldBe thrownBy { singleBase.updated(13, -2) }
      val singleBaseBytes = singleBase.unsafeToBytes

      singleBase.updated(13, 56) shouldBe theSameInstanceAs(singleBase)

      withClue("(13 -> 57).updated(13, 57)") {
        val updated = singleBase.updated(13, 57)
        updated.size shouldBe singleBase.size
        updated.get(13) should contain(57)
        val updatedBytes = updated.unsafeToBytes
        updatedBytes.length shouldBe singleBaseBytes.length
        (0 until (updatedBytes.length - 1)).foreach { i =>
          updatedBytes(i) shouldBe singleBaseBytes(i)
        }
        updatedBytes(updatedBytes.length - 1) shouldBe 57
        singleBaseBytes(updatedBytes.length - 1) shouldBe 56

        singleBase.locateSequenceNr(57) shouldBe empty
        updated.locateSequenceNr(57) should contain(13)

        equalityContract(updated, MultiJournalMetadata(Map(13 -> 57)))
      }

      withClue("(13 -> 56).updated(14, 50)") {
        val iae = the[IllegalArgumentException] thrownBy { singleBase.updated(14, 50) }
        iae.getMessage should include("would break monotonicity: [56, 50]")
      }

      withClue("(13 -> 56).updated(12, 60)") {
        val iae = the[IllegalArgumentException] thrownBy { singleBase.updated(12, 60) }
        iae.getMessage should include("would break monotonicity: [60, 56]")
      }

      withClue("(13 -> 56).updated(14, 75)") {
        val updated = singleBase.updated(14, 75)
        updated.size shouldBe 2
        updated.bounds shouldBe (12 -> 14)
        updated.get(13) should contain(56)
        updated.get(14) should contain(75)
        val updatedBytes = updated.unsafeToBytes
        updatedBytes.length shouldBe 12 * updated.size
        (updatedBytes.take(12) should contain).theSameElementsInOrderAs(singleBaseBytes)
        equalityContract(updated, MultiJournalMetadata(Map(13 -> 56, 14 -> 75)))
      }

      withClue("(13 -> 56).updated(16, 100)") {
        val updated = singleBase.updated(16, 100)
        updated.size shouldBe 4
        updated.bounds shouldBe (12 -> 16)
        ((13 to 17).map(updated.get) should contain)
          .theSameElementsInOrderAs(Seq(Seq.fill(3)(Some(56)), Seq(Some(100)), Seq(None)).flatten)
        ((1L to 101).map(updated.locateSequenceNr) should contain)
          .theSameElementsInOrderAs(Seq(Seq.fill(56)(Some(13)), Seq.fill(44)(Some(16)), Seq(None)).flatten)
        equalityContract(updated, MultiJournalMetadata(Map(13 -> 56, 16 -> 100)))
      }

      withClue("(13 -> 56).updated(12, 25)") {
        val updated = singleBase.updated(12, 25)
        updated.size shouldBe 2
        updated.bounds shouldBe (11 -> 13)
        updated.get(12) should contain(25)
        updated.get(13) should contain(56)
        equalityContract(updated, MultiJournalMetadata(Map(13 -> 56, 12 -> 25)))
      }

      withClue("(13 -> 56).updated(10, 40)") {
        val updated = singleBase.updated(10, 40)
        updated.size shouldBe 4
        updated.bounds shouldBe (9 -> 13)
        ((10 to 14).map(updated.get) should contain)
          .theSameElementsInOrderAs(Seq(Seq.fill(3)(Some(40)), Seq(Some(56)), Seq(None)).flatten)
        ((1L to 57).map(updated.locateSequenceNr) should contain)
          .theSameElementsInOrderAs(Seq(Seq.fill(40)(Some(10)), Seq.fill(16)(Some(13)), Some(None)).flatten)
        equalityContract(updated, MultiJournalMetadata(Map(13 -> 56, 10 -> 40)))
      }

      val doubleBase = MultiJournalMetadata(Map(21 -> 12, 24 -> 17))

      withClue("(21 -> 12, 24 -> 17).updated(23, 15)") {
        val updated = doubleBase.updated(23, 15)
        updated.size shouldBe doubleBase.size
        updated.bounds shouldBe doubleBase.bounds
        ((20 to 25).map(updated.get) should contain)
          .theSameElementsInOrderAs(Seq(None, Some(12), Some(12), Some(15), Some(17), None))
        equalityContract(updated, MultiJournalMetadata(Map(21 -> 12, 23 -> 15, 24 -> 17)))
      }

      withClue("(21 -> 12, 24 -> 17).updated(22, 15)") {
        val iae = the[IllegalArgumentException] thrownBy { doubleBase.updated(22, 15) }
        iae.getMessage should include("would break monotonicity: [12, 15, 12]")
      }

      withClue("(21 -> 12, 24 -> 17).updated(18, 7)") {
        val updated = doubleBase.updated(18, 7)
        updated.size shouldBe (doubleBase.size + 3)
        updated.bounds shouldBe (17 -> 24)
        ((17 to 25).map(updated.get) should contain)
          .theSameElementsInOrderAs(
            Seq(Seq(None), Seq.fill(3)(Some(7)), Seq.fill(3)(Some(12)), Seq(Some(17)), Seq(None)).flatten)

        ((1L to 18).map(updated.locateSequenceNr) should contain)
          .theSameElementsInOrderAs(
            Seq(Seq.fill(7)(Some(18)), Seq.fill(5)(Some(21)), Seq.fill(5)(Some(24)), Seq(None)).flatten)

        equalityContract(updated, MultiJournalMetadata(Map(18 -> 7, 21 -> 12, 24 -> 17)))
      }

      withClue("(21 -> 12, 24 -> 17).updated(27, 28)") {
        val updated = doubleBase.updated(27, 28)
        updated.size shouldBe (doubleBase.size + 3)
        updated.bounds shouldBe (20 -> 27)
        ((21 to 28).map(updated.get) should contain)
          .theSameElementsInOrderAs(Seq(Seq.fill(3)(Some(12)), Seq.fill(3)(Some(17)), Seq(Some(28)), Seq(None)).flatten)
        ((1L to 29).map(updated.locateSequenceNr) should contain)
          .theSameElementsInOrderAs(
            Seq(Seq.fill(12)(Some(21)), Seq.fill(5)(Some(24)), Seq.fill(11)(Some(27)), Seq(None)).flatten)

        equalityContract(updated, MultiJournalMetadata(Map(21 -> 12, 24 -> 17, 27 -> 28)))
      }
    }
  }

  "MultiJournalMetadata serializer" should {
    "throw on misregistered classes" in {
      (the[IllegalArgumentException] thrownBy { serializer.manifest(new AnyRef) }).getMessage should startWith(
        "misregistered class")

      (the[IllegalArgumentException] thrownBy { serializer.toBinary(new AnyRef) }).getMessage should startWith(
        "misregistered class")
    }

    "emit the correct manifest" in {
      serializer.manifest(MultiJournalMetadata.Empty) shouldBe "MultiJournalMetadata"
    }

    "reject unknown manifests" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        serializer.fromBinary(new Array[Byte](42), "Not a MultiJournalManifest")
      }
    }

    "reject too-short payloads" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        serializer.fromBinary(new Array[Byte](7), "MultiJournalMetadata")
      }
    }

    "reject payloads that aren't a multiple of 12" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        serializer.fromBinary(new Array[Byte](18), "MultiJournalMetadata")
      }
    }

    "reject payloads with negative journalIds" in {
      val baos = new ByteArrayOutputStream
      val os = new DataOutputStream(baos)
      try {
        os.writeInt(42)
        os.writeLong(0xdeadbeef)
        os.writeInt(-7)
        os.writeLong(0x414b4b41)
        an[IllegalArgumentException] shouldBe thrownBy {
          serializer.fromBinary(baos.toByteArray(), "MultiJournalMetadata")
        }
      } finally os.close()
    }

    "reject non-ordered payloads" in {
      val baos = new ByteArrayOutputStream
      val os = new DataOutputStream(baos)
      try {
        os.writeInt(0x4b4c)
        os.writeLong(22)
        os.writeInt(0x414b)
        os.writeLong(17)
        an[IllegalArgumentException] shouldBe thrownBy {
          serializer.fromBinary(baos.toByteArray(), "MultiJournalMetadata")
        }
      } finally os.close()
    }

    "reject non-monotonic payloads" in {
      val baos = new ByteArrayOutputStream
      val os = new DataOutputStream(baos)
      try {
        os.writeInt(0x4b41)
        os.writeLong(22)
        os.writeInt(0x4b42)
        os.writeLong(17)
        an[IllegalArgumentException] shouldBe thrownBy {
          serializer.fromBinary(baos.toByteArray(), "MultiJournalMetadata")
        }
      } finally os.close()
    }

    "reject gapped payloads" in {
      val baos = new ByteArrayOutputStream
      val os = new DataOutputStream(baos)
      try {
        os.writeInt(0x414b)
        os.writeLong(17)
        os.writeInt(0x4b41)
        os.writeLong(22)
        an[IllegalArgumentException] shouldBe thrownBy {
          serializer.fromBinary(baos.toByteArray(), "MultiJournalMetadata")
        }
      } finally os.close()
    }

    "accept montonic gap-less payloads" in {
      val baos = new ByteArrayOutputStream
      val os = new DataOutputStream(baos)
      try {
        os.writeInt(0x414b)
        os.writeLong(17)
        os.writeInt(0x414c)
        os.writeLong(17)
        os.writeInt(0x414d)
        os.writeLong(170)
        serializer.fromBinary(baos.toByteArray(), "MultiJournalMetadata") shouldEqual MultiJournalMetadata(
          Map(0x414b -> 17, 0x414d -> 170))
      } finally os.close()
    }

    "serialize in journal order" in {
      val highSeqs = Map[Int, Long](3 -> 127, 5 -> (127 * 127), 8 -> (127 * 127 * 127 * 127))
      val metadata = MultiJournalMetadata(highSeqs)
      val bytes = serializer.toBinary(metadata)
      val is = new DataInputStream(new ByteArrayInputStream(bytes))
      val lowJournal = highSeqs.keys.head
      val highJournal = highSeqs.keys.last
      bytes.length shouldBe (12 * (1 + highJournal - lowJournal))

      (lowJournal to highJournal).foreach { journalId =>
        withClue(journalId) {
          is.readInt() shouldBe journalId
          is.readLong() shouldBe metadata(journalId)
        }
      }
    }
  }

  private def equalityContract(actual: MultiJournalMetadata, expected: MultiJournalMetadata): Unit = {
    actual shouldEqual expected
    actual.hashCode shouldBe expected.hashCode
    val manifest = serializer.manifest(actual)
    val bytes = serializer.toBinary(actual)
    val deserialized = serializer.fromBinary(bytes, manifest)
    deserialized shouldEqual actual
    deserialized.hashCode shouldBe actual.hashCode
  }
}
