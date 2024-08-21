/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.query

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset.toTimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.dynamodb.TestActors
import akka.persistence.dynamodb.TestActors.Persister
import akka.persistence.dynamodb.TestActors.Persister.PersistWithAck
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object EventsBySliceStartingFromSnapshotSpec {
  sealed trait QueryType
  case object Live extends QueryType
  case object Current extends QueryType

  def config: Config =
    TestConfig.backtrackingDisabledConfig
      .withFallback(ConfigFactory.parseString(s"""
        akka.persistence.dynamodb.query.start-from-snapshot.enabled = true
        akka.persistence.dynamodb.query.refresh-interval = 1s

        # This test is not using backtracking, so increase behind-current-time to
        # reduce risk of missing events
        akka.persistence.dynamodb.query.behind-current-time = 500 millis

        akka.persistence.dynamodb.journal.publish-events = off
        """))
      .withFallback(TestConfig.config)
      .resolve()
}

class EventsBySliceStartingFromSnapshotSpec
    extends ScalaTestWithActorTestKit(EventsBySliceStartingFromSnapshotSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventsBySliceStartingFromSnapshotSpec._

  override def typedSystem: ActorSystem[_] = system

  private val query =
    PersistenceQuery(testKit.system).readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)

  // Snapshots of the Persister is string concatenation of the events
  private def expectedSnapshotEvent(seqNr: Long): String =
    (1L to seqNr).map(i => s"e-$i").mkString("|") + "-snap"

  private class Setup {
    val entityType = nextEntityType()
    val persistenceId = nextPersistenceId(entityType)
    val slice = query.sliceForPersistenceId(persistenceId.id)
    val snapshotAckProbe = createTestProbe[Long]()
    val persister = spawn(TestActors.Persister.withSnapshotAck(persistenceId, Set.empty, snapshotAckProbe.ref))
    val probe = createTestProbe[Done]()
    val sinkProbe = TestSink[EventEnvelope[String]]()
  }

  List[QueryType](Current, Live).foreach { queryType =>
    def doQuery(
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        offset: Offset,
        queryImpl: DynamoDBReadJournal = query): Source[EventEnvelope[String], NotUsed] =
      queryType match {
        case Live =>
          queryImpl.eventsBySlicesStartingFromSnapshots[String, String](
            entityType,
            minSlice,
            maxSlice,
            offset,
            snap => snap)
        case Current =>
          queryImpl.currentEventsBySlicesStartingFromSnapshots[String, String](
            entityType,
            minSlice,
            maxSlice,
            offset,
            snap => snap)
      }

    def assertFinished(probe: TestSubscriber.Probe[EventEnvelope[String]]): Unit =
      queryType match {
        case Live =>
          probe.expectNoMessage()
          probe.cancel()
        case Current =>
          probe.expectComplete()
      }

    s"$queryType eventsBySlicesStartingFromSnapshots" should {
      "return all snapshots and events for NoOffset" in new Setup {
        for (i <- 1 to 20) {
          if (i == 17) {
            persister ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(17L)
          } else
            persister ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }
        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset)
            .runWith(sinkProbe)
            .request(21)

        result.expectNext().event shouldBe expectedSnapshotEvent(17)
        for (i <- 18 to 20) {
          result.expectNext().event shouldBe s"e-$i"
        }
        assertFinished(result)
      }

      "only return snapshots and events after an offset" in new Setup {
        for (i <- 1 to 20) {
          if (i == 3) {
            persister ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(3L)
          } else
            persister ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }

        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset)
            .runWith(sinkProbe)
            .request(21)

        // we want to start next query from offset corresponding to 10
        // first two events are not included because of snapshot at 3
        result.expectNextN(9 - 2)
        val offset = result.expectNext().offset
        result.cancel()

        val withOffset =
          doQuery(entityType, slice, slice, offset)
            .runWith(TestSink[EventEnvelope[String]]())
        withOffset.request(12)
        for (i <- 11 to 20) {
          // doesn't include the snapshot because that was before the offset
          withOffset.expectNext().event shouldBe s"e-$i"
        }
        assertFinished(withOffset)
      }

      "use same timestamp for the snapshot as the event" in new Setup {
        for (i <- 1 to 20) {
          if (i == 17) {
            persister ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(17L)
          } else
            persister ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }
        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset)
            .runWith(sinkProbe)
            .request(21)

        val snapEnvelope = result.expectNextN(3).head
        val eventEnvelope = query.loadEnvelope[String](snapEnvelope.persistenceId, snapEnvelope.sequenceNr).futureValue
        toTimestampOffset(snapEnvelope.offset).timestamp shouldBe toTimestampOffset(eventEnvelope.offset).timestamp
        // also verify that the seen Map is populated
        toTimestampOffset(snapEnvelope.offset).seen(snapEnvelope.persistenceId) shouldBe (snapEnvelope.sequenceNr)

        result.cancel()
      }

      "includes tags in snapshot" in new Setup {
        val taggingPersister: ActorRef[Persister.Command] =
          spawn(TestActors.Persister.withSnapshotAck(persistenceId, tags = Set("tag-A"), snapshotAckProbe.ref))

        taggingPersister ! PersistWithAck(s"e-1", probe.ref)
        probe.expectMessage(Done)
        taggingPersister ! PersistWithAck(s"e-2-snap", probe.ref)
        snapshotAckProbe.expectMessage(2L)
        probe.expectMessage(Done)
        taggingPersister ! PersistWithAck(s"e-3", probe.ref)
        probe.expectMessage(Done)

        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset)
            .runWith(TestSink())
        result.request(2)

        val snapshotEnvelope = result.expectNext()
        snapshotEnvelope.event shouldBe expectedSnapshotEvent(2)
        snapshotEnvelope.tags shouldBe Set("tag-A")

        val e3Envelope = result.expectNext()
        e3Envelope.event shouldBe "e-3"
        e3Envelope.tags shouldBe Set("tag-A")

        assertFinished(result)
      }

    }
  }

  // tests just relevant for current query
  "Current eventsBySlicesStartingFromSnapshots" should {
    "retrieve from several slices" in new Setup {
      val numberOfPersisters = 20
      val numberOfEvents = 3
      val persistenceIds = (1 to numberOfPersisters).map(_ => nextPersistenceId(entityType)).toVector
      persistenceIds.foreach { pid =>
        val ref = testKit.spawn(TestActors.Persister.withSnapshotAck(pid, Set.empty, snapshotAckProbe.ref))
        for (i <- 1 to numberOfEvents) {
          if (i == 2) {
            ref ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(i.toLong)
          } else
            ref ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }
      }

      persistenceExt.numberOfSlices should be(1024)
      val ranges = query.sliceRanges(4)
      ranges(0) should be(0 to 255)
      ranges(1) should be(256 to 511)
      ranges(2) should be(512 to 767)
      ranges(3) should be(768 to 1023)

      val allEnvelopes =
        (0 until 4).flatMap { rangeIndex =>
          val result =
            query
              .currentEventsBySlicesStartingFromSnapshots[String, String](
                entityType,
                ranges(rangeIndex).min,
                ranges(rangeIndex).max,
                NoOffset,
                snap => snap)
              .runWith(Sink.seq)
              .futureValue
          result.foreach { env =>
            ranges(rangeIndex) should contain(query.sliceForPersistenceId(env.persistenceId))
          }
          result
        }

      val numberOfEventsBeforeSnapshots = numberOfPersisters
      allEnvelopes.size should be(numberOfPersisters * numberOfEvents - numberOfEventsBeforeSnapshots)
    }
  }

  // tests just relevant for live query
  "Live eventsBySlicesStartingFromSnapshots" should {
    "find new events" in new Setup {
      for (i <- 1 to 20) {
        if (i == 17) {
          persister ! PersistWithAck(s"e-$i-snap", probe.ref)
          snapshotAckProbe.expectMessage(17L)
        } else
          persister ! PersistWithAck(s"e-$i", probe.ref)
        probe.expectMessage(Done)
      }
      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlicesStartingFromSnapshots[String, String](entityType, slice, slice, NoOffset, snap => snap)
          .runWith(sinkProbe)
          .request(21)

      result.expectNext().event shouldBe expectedSnapshotEvent(17)
      for (i <- 18 to 20) {
        result.expectNext().event shouldBe s"e-$i"
      }

      for (i <- 21 to 40) {
        persister ! PersistWithAck(s"e-$i", probe.ref)
        // make sure the query doesn't get an element in its buffer with nothing to take it
        // resulting in it not finishing the query and giving up the session
        result.request(1)
        probe.expectMessage(Done)
      }

      result.request(1)

      for (i <- 21 to 40) {
        result.expectNext().event shouldBe s"e-$i"
      }

      result.cancel()
    }

    "retrieve from several slices" in new Setup {
      val numberOfPersisters = 20
      val numberOfEventsPhase1 = 3
      val numberOfEventsPhase2 = 2

      val persistenceIds = (1 to numberOfPersisters)
        .map(_ => nextPersistenceId(entityType))
        .toVector
      val persisters = persistenceIds.map { pid =>
        val ref = testKit.spawn(TestActors.Persister.withSnapshotAck(pid, Set.empty, snapshotAckProbe.ref))
        for (i <- 1 to numberOfEventsPhase1) {
          if (i == numberOfEventsPhase1) {
            ref ! PersistWithAck(s"e-$i-snap", probe.ref)
            probe.expectMessage(Done)
            snapshotAckProbe.expectMessage(i.toLong)
          } else {
            ref ! PersistWithAck(s"e-$i", probe.ref)
            probe.expectMessage(Done)
          }
        }
        ref
      }

      persistenceExt.numberOfSlices should be(1024)
      val ranges = query.sliceRanges(4)
      ranges(0) should be(0 to 255)
      ranges(1) should be(256 to 511)
      ranges(2) should be(512 to 767)
      ranges(3) should be(768 to 1023)

      val queries: Seq[Source[EventEnvelope[String], NotUsed]] =
        (0 until 4).map { rangeIndex =>
          query
            .eventsBySlicesStartingFromSnapshots[String, String](
              entityType,
              ranges(rangeIndex).min,
              ranges(rangeIndex).max,
              NoOffset,
              snap => snap)
            .map { env =>
              ranges(rangeIndex) should contain(query.sliceForPersistenceId(env.persistenceId))
              env
            }
        }

      // events before the snapshot are not included
      val numberOfSnapshots = numberOfPersisters
      val expectedNumberOfEvents = numberOfSnapshots + numberOfPersisters * numberOfEventsPhase2

      val allEnvelopes =
        queries(0)
          .merge(queries(1))
          .merge(queries(2))
          .merge(queries(3))
          .take(expectedNumberOfEvents)
          .runWith(Sink.seq[EventEnvelope[String]])

      persisters.foreach { ref =>
        for (i <- 1 to numberOfEventsPhase2) {
          ref ! PersistWithAck(s"e-${numberOfEventsPhase1 + i}", probe.ref)
          probe.expectMessage(Done)
        }
      }

      allEnvelopes.futureValue.size should be(expectedNumberOfEvents)
    }
  }

}
