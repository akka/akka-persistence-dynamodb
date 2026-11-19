/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal.query.scaladsl

import java.time.Instant
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import scala.jdk.CollectionConverters.SeqHasAsJava

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.japi.Pair
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.ReadJournalProvider
import akka.persistence.query.javadsl.{ ReadJournal => JavaReadJournal }
import akka.persistence.query.scaladsl.{ ReadJournal => ScalaReadJournal }
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.javadsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.EventsBySliceQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object JavaReadJournalAdaptationSpec {
  val Config: Config = ConfigFactory
    .parseString(s"""
      java-only-query.class = "akka.persistence.multijournal.query.scaladsl.JavaOnlyReadJournalProvider"

      java-adapting-multi = $${akka.persistence.multi-journal.query}
      java-adapting-multi.read-journals.0 = "java-only-query"
      """)
    .withFallback(ConfigFactory.load())
    .resolve()
}

final class JavaOnlyReadJournalProvider(system: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {
  private val scalaInstance = new ScalaReadJournal {}
  private val javaInstance = new JavaOnlyReadJournal

  override def scaladslReadJournal(): ScalaReadJournal = scalaInstance
  override def javadslReadJournal(): JavaReadJournal = javaInstance
}

final class JavaOnlyReadJournal
    extends JavaReadJournal
    with CurrentEventsByPersistenceIdTypedQuery
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery
    with EventTimestampQuery
    with LoadEventQuery {
  private val events = 1L to 3L

  private def envelope[Event](persistenceId: String, sequenceNr: Long): EventEnvelope[Event] =
    EventEnvelope(
      NoOffset,
      persistenceId,
      sequenceNr,
      s"event-$sequenceNr".asInstanceOf[Event],
      sequenceNr,
      "entity",
      slice = 0)

  private def source[Event](persistenceId: String, sequenceNrs: Seq[Long]): Source[EventEnvelope[Event], NotUsed] =
    akka.stream.scaladsl.Source(sequenceNrs).map(envelope[Event](persistenceId, _)).asJava

  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
    source(persistenceId, events.filter(sequenceNr => sequenceNr >= fromSequenceNr && sequenceNr <= toSequenceNr))

  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    source("entity|id", events)

  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    source("entity|id", events)

  override def sliceForPersistenceId(persistenceId: String): Int = 0

  override def sliceRanges(numberOfRanges: Int): java.util.List[Pair[Integer, Integer]] =
    Seq(Pair(Integer.valueOf(0), Integer.valueOf(1023))).asJava

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    CompletableFuture.completedFuture(
      if (events.contains(sequenceNr)) Optional.of(Instant.ofEpochMilli(sequenceNr)) else Optional.empty())

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    if (events.contains(sequenceNr)) CompletableFuture.completedFuture(envelope(persistenceId, sequenceNr))
    else CompletableFuture.failedFuture(new NoSuchElementException)
}

class JavaReadJournalAdaptationSpec
    extends ScalaTestWithActorTestKit(JavaReadJournalAdaptationSpec.Config)
    with AnyWordSpecLike
    with LogCapturing {
  private lazy val query =
    PersistenceQuery(system).readJournalFor[MultiJournalReadJournal]("java-adapting-multi")

  "A multi-journal read journal" should {
    "adapt query capabilities exposed only through the Java DSL" in {
      query
        .currentEventsByPersistenceIdTyped[String]("entity|id", 2, 3)
        .runWith(Sink.seq)
        .futureValue
        .map(_.event) shouldBe Seq("event-2", "event-3")

      query
        .currentEventsBySlices[String]("entity", 0, 0, NoOffset)
        .runWith(Sink.seq)
        .futureValue
        .map(_.event) shouldBe Seq("event-1", "event-2", "event-3")

      query
        .eventsBySlices[String]("entity", 0, 0, NoOffset)
        .runWith(Sink.seq)
        .futureValue
        .map(_.event) shouldBe Seq("event-1", "event-2", "event-3")

      query.timestampOf("entity|id", 2).futureValue shouldBe Some(Instant.ofEpochMilli(2))
      query.timestampOf("entity|id", 4).futureValue shouldBe None
      query.loadEnvelope[String]("entity|id", 3).futureValue.event shouldBe "event-3"
      query.loadEnvelope[String]("entity|id", 4).failed.futureValue shouldBe a[NoSuchElementException]
    }
  }
}
