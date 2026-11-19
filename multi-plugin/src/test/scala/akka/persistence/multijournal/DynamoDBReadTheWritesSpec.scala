/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

import akka.actor.ActorRef
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.Persistence
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.util.ClientProvider
import akka.persistence.dynamodb.util.scaladsl.CreateTables
import akka.persistence.multijournal.query.javadsl.{ MultiJournalReadJournal => JavaMultiJournalReadJournal }
import akka.persistence.multijournal.query.scaladsl.MultiJournalReadJournal
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffsetBySlice
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

object DynamoDBReadTheWritesSpec {
  val Config: Config = ConfigFactory
    .parseString(s"""
    |akka.persistence.dynamodb.client.local.enabled = true
    |akka.persistence.dynamodb.journal.publish-events = off
    |akka.actor.testkit.typed.default-timeout = 10s
    |
    |cold-dynamodb = $${akka.persistence.dynamodb}
    |cold-dynamodb.journal.table = "multi_journal_cold"
    |
    |lukewarm-dynamodb = $${akka.persistence.dynamodb}
    |lukewarm-dynamodb.journal.table = "multi_journal_lukewarm"
    |
    |hot-dynamodb = $${akka.persistence.dynamodb}
    |hot-dynamodb.journal.table = "multi_journal_hot"
    |
    |two-dynamodb-journal = $${akka.persistence.multi-journal}
    |two-dynamodb-journal.journals {
    |  0 = "cold-dynamodb.journal"
    |  1 = "lukewarm-dynamodb.journal"
    |}
    |
    |three-dynamodb-journal = $${akka.persistence.multi-journal}
    |three-dynamodb-journal.journals {
    |  0 = "cold-dynamodb.journal"
    |  1 = "lukewarm-dynamodb.journal"
    |  2 = "hot-dynamodb.journal"
    |}
    |three-dynamodb-journal.query.read-journals {
    |  0 = "cold-dynamodb.query"
    |  1 = "lukewarm-dynamodb.query"
    |  2 = "hot-dynamodb.query"
    |}
    |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  val DynamoDBConfigPaths = Seq("cold-dynamodb", "lukewarm-dynamodb", "hot-dynamodb")
}

class DynamoDBReadTheWritesSpec
    extends ScalaTestWithActorTestKit(DynamoDBReadTheWritesSpec.Config)
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with LogCapturing
    with TestData {
  import DynamoDBReadTheWritesSpec._
  import ReadTheWritesSpec.TestActor
  import TestActor._

  private lazy val query =
    PersistenceQuery(system).readJournalFor[MultiJournalReadJournal]("three-dynamodb-journal.query")
  private lazy val javaQuery =
    PersistenceQuery(system).getReadJournalFor(classOf[JavaMultiJournalReadJournal], "three-dynamodb-journal.query")
  private lazy val twoJournal = Persistence(system.classicSystem).journalFor("two-dynamodb-journal")
  private lazy val threeJournal = Persistence(system.classicSystem).journalFor("three-dynamodb-journal")

  private def getMetadata(journal: ActorRef, pid: String): Future[Map[Int, Long]] = {
    val promise = Promise[Map[Int, Long]]()
    journal ! MultiJournal.GetMetadata(pid, promise)
    promise.future
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    DynamoDBConfigPaths.foreach { configPath =>
      val settings = DynamoDBSettings(system.settings.config.getConfig(configPath))
      val client = ClientProvider(system).clientFor(s"$configPath.client")
      Await.result(CreateTables.createJournalTable(system, settings, client, deleteIfExists = true), 10.seconds)
    }
  }

  "MultiJournal backed by DynamoDB" should {
    "read events written before and during journal migrations" in {
      val pid = nextPid()
      val stateProbe = createTestProbe[Vector[String]]()

      val beforeMigration = spawn(TestActor(pid, "cold-dynamodb.journal"))
      beforeMigration ! Persist(Seq("cold-1", "cold-2"), stateProbe.ref)
      stateProbe.expectMessage(Vector("cold-1", "cold-2"))
      testKit.stop(beforeMigration)

      val withLukewarm = spawn(TestActor(pid, "two-dynamodb-journal"))
      withLukewarm ! GetState(stateProbe.ref)
      stateProbe.expectMessage(Vector("cold-1", "cold-2"))
      getMetadata(twoJournal, pid).futureValue shouldBe Map(0 -> 2L)
      withLukewarm ! Persist(Seq("lukewarm-3", "lukewarm-4"), stateProbe.ref)
      stateProbe.expectMessage(Vector("cold-1", "cold-2", "lukewarm-3", "lukewarm-4"))
      getMetadata(twoJournal, pid).futureValue shouldBe Map(0 -> 2L)
      testKit.stop(withLukewarm)

      val withHot = spawn(TestActor(pid, "three-dynamodb-journal"))
      withHot ! GetState(stateProbe.ref)
      stateProbe.expectMessage(Vector("cold-1", "cold-2", "lukewarm-3", "lukewarm-4"))
      getMetadata(threeJournal, pid).futureValue shouldBe Map(0 -> 2L, 1 -> 4L)
      withHot ! Persist(Seq("hot-5", "hot-6"), stateProbe.ref)
      val allEvents = Vector("cold-1", "cold-2", "lukewarm-3", "lukewarm-4", "hot-5", "hot-6")
      stateProbe.expectMessage(allEvents)
      getMetadata(threeJournal, pid).futureValue shouldBe Map(0 -> 2L, 1 -> 4L)
      testKit.stop(withHot)

      val restarted = spawn(TestActor(pid, "three-dynamodb-journal"))
      restarted ! GetState(stateProbe.ref)
      stateProbe.expectMessage(allEvents)
      getMetadata(threeJournal, pid).futureValue shouldBe Map(0 -> 2L, 1 -> 4L)
      testKit.stop(restarted)
    }
  }

  "MultiJournal queries backed by DynamoDB" should {
    "read events by persistence ID across all journals" in {
      val (pid, allEvents) = writeAcrossJournals()

      val envelopes = query
        .currentEventsByPersistenceIdTyped[String](pid, 2, 5)
        .runWith(Sink.seq)
        .futureValue

      envelopes.map(_.event) shouldBe allEvents.slice(1, 5)
      envelopes.map(_.sequenceNr) shouldBe (2L to 5L)
      envelopes.foreach(_.metadata[MultiJournalMetadata] shouldBe empty)
    }

    "merge events by slice and resume from a TimestampOffsetBySlice" in {
      val (pid, allEvents) = writeAcrossJournals()
      val entityType = PersistenceId.extractEntityType(pid)
      val slice = query.sliceForPersistenceId(pid)

      val envelopes = query
        .currentEventsBySlices[String](entityType, slice, slice, NoOffset)
        .runWith(Sink.seq)
        .futureValue

      envelopes.map(_.event) shouldBe allEvents
      envelopes.map(_.sequenceNr) shouldBe (1L to 6L)
      envelopes.foreach(_.metadata[MultiJournalMetadata] shouldBe empty)

      val offset = envelopes(3).offset.asInstanceOf[TimestampOffset]
      val resumed = query
        .currentEventsBySlices[String](entityType, slice, slice, TimestampOffsetBySlice(Map(slice -> offset)))
        .runWith(Sink.seq)
        .futureValue

      resumed.map(_.event) shouldBe allEvents.drop(4)
      resumed.map(_.sequenceNr) shouldBe Seq(5L, 6L)
    }

    "look up timestamps and envelopes across all journals" in {
      val (pid, allEvents) = writeAcrossJournals()

      Seq(1L, 3L, 5L).foreach { sequenceNr =>
        query.timestampOf(pid, sequenceNr).futureValue shouldBe defined
        val envelope = query.loadEnvelope[String](pid, sequenceNr).futureValue
        envelope.event shouldBe allEvents((sequenceNr - 1).toInt)
        envelope.metadata[MultiJournalMetadata] shouldBe empty
      }
      query.timestampOf(pid, 7L).futureValue shouldBe empty
      query.loadEnvelope[String](pid, 7L).failed.futureValue shouldBe a[NoSuchElementException]
    }

    "merge the live hot query with finite cooler queries" in {
      val (pid, allEvents) = writeAcrossJournals()
      val entityType = PersistenceId.extractEntityType(pid)
      val slice = query.sliceForPersistenceId(pid)

      val envelopes = query
        .eventsBySlices[String](entityType, slice, slice, NoOffset)
        .take(allEvents.size.toLong)
        .runWith(Sink.seq)
        .futureValue

      envelopes.map(_.event) shouldBe allEvents
      envelopes.map(_.sequenceNr) shouldBe (1L to 6L)
      envelopes.foreach(_.metadata[MultiJournalMetadata] shouldBe empty)
    }

    "load and delegate through the Java DSL provider" in {
      val (pid, allEvents) = writeAcrossJournals()

      val envelopes = javaQuery
        .currentEventsByPersistenceIdTyped[String](pid, 1, Long.MaxValue)
        .asScala
        .runWith(Sink.seq)
        .futureValue

      envelopes.map(_.event) shouldBe allEvents
      javaQuery.sliceForPersistenceId(pid) shouldBe query.sliceForPersistenceId(pid)
      javaQuery.sliceRanges(4).size shouldBe 4
    }
  }

  private def writeAcrossJournals(): (String, Vector[String]) = {
    val pid = PersistenceId("ReadTheWrites", nextPid()).id
    val stateProbe = createTestProbe[Vector[String]]()

    val beforeMigration = spawn(TestActor(pid, "cold-dynamodb.journal"))
    beforeMigration ! Persist(Seq("cold-1", "cold-2"), stateProbe.ref)
    stateProbe.expectMessage(Vector("cold-1", "cold-2"))
    testKit.stop(beforeMigration)

    val withLukewarm = spawn(TestActor(pid, "two-dynamodb-journal"))
    withLukewarm ! Persist(Seq("lukewarm-3", "lukewarm-4"), stateProbe.ref)
    stateProbe.expectMessage(Vector("cold-1", "cold-2", "lukewarm-3", "lukewarm-4"))
    testKit.stop(withLukewarm)

    val withHot = spawn(TestActor(pid, "three-dynamodb-journal"))
    withHot ! Persist(Seq("hot-5", "hot-6"), stateProbe.ref)
    val allEvents = Vector("cold-1", "cold-2", "lukewarm-3", "lukewarm-4", "hot-5", "hot-6")
    stateProbe.expectMessage(allEvents)
    testKit.stop(withHot)

    pid -> allEvents
  }
}
