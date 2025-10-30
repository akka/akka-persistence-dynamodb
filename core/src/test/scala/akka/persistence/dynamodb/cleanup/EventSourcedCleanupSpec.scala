/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.cleanup

import java.time.Instant

import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.dynamodb.TestActors.Persister
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.persistence.dynamodb.cleanup.scaladsl.EventSourcedCleanup
import com.typesafe.config.Config
import org.scalatest.Inspectors
import org.scalatest.OptionValues
import org.slf4j.event.Level

object EventSourcedCleanupSpec {
  val config: Config = ConfigFactory
    .parseString(s"""
    akka.loglevel = DEBUG
    akka.persistence.dynamodb.cleanup {
      log-progress-every = 2
    }
  """)
    .withFallback(TestConfig.config)
}

class EventSourcedCleanupSpec
    extends ScalaTestWithActorTestKit(EventSourcedCleanupSpec.config)
    with AnyWordSpecLike
    with Inspectors
    with OptionValues
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  "EventSourcedCleanup" must {
    "delete all events for one persistenceId" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      val pid = nextPid()
      val p = spawn(Persister(pid))

      (1 to 10).foreach { n =>
        p ! Persister.PersistWithAck(n, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(p)

      val cleanup = new EventSourcedCleanup(system)
      cleanup.deleteAllEvents(pid, resetSequenceNumber = true).futureValue

      val p2 = spawn(Persister(pid))
      p2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("")
      p2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(0L)
    }

    "delete all events for one persistenceId in batches" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      val pid = nextPid()
      val p = spawn(Persister(pid))

      val maxSeqNumber = 321
      (1 to maxSeqNumber).foreach { n =>
        p ! Persister.PersistWithAck(n, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(p)

      val cleanup = new EventSourcedCleanup(system)

      var iteration = 0
      val batchSize = 100 // hard-coded TransactWriteItems limit

      LoggingTestKit
        .info("Deleted")
        .withLogLevel(Level.DEBUG)
        .withOccurrences(4)
        .withCustom { event =>
          val from = (iteration * batchSize) + 1
          iteration = iteration + 1
          val to = Math.min(maxSeqNumber, from + batchSize - 1)
          val expectedMsg = s"Deleted events from [$from] to [$to] for persistenceId [$pid], consumed [4.0] WCU"
          event.message == expectedMsg
        }
        .expect {
          cleanup.deleteAllEvents(pid, resetSequenceNumber = true).futureValue
        }

      val p2 = spawn(Persister(pid))
      p2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("")
      p2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(0L)
    }

    "delete all events for one persistenceId, but keep seqNr" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      val pid = nextPid()
      val p = spawn(Persister(pid))

      (1 to 10).foreach { n =>
        p ! Persister.PersistWithAck(n, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(p)

      val cleanup = new EventSourcedCleanup(system)
      cleanup.deleteAllEvents(pid, resetSequenceNumber = false).futureValue

      val p2 = spawn(Persister(pid))
      p2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("")
      p2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(10L)
    }

    "delete all events for one persistenceId in batches, but keep seqNr" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      val pid = nextPid()
      val p = spawn(Persister(pid))

      val maxSeqNumber = 321
      (1 to maxSeqNumber).foreach { n =>
        p ! Persister.PersistWithAck(n, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(p)

      val cleanup = new EventSourcedCleanup(system)

      var iteration = 0
      val batchSize = 100 // hard-coded TransactWriteItems limit

      LoggingTestKit
        .info("Deleted")
        .withLogLevel(Level.DEBUG)
        .withOccurrences(4)
        .withCustom { event =>
          val from = (iteration * batchSize) + 1
          iteration = iteration + 1
          val to = Math.min(maxSeqNumber, from + batchSize - 1)
          val expectedMsg = s"Deleted events from [$from] to [$to] for persistenceId [$pid], consumed [4.0] WCU"
          event.message == expectedMsg
        }
        .expect {
          cleanup.deleteAllEvents(pid, resetSequenceNumber = false).futureValue
        }

      val p2 = spawn(Persister(pid))
      p2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("")
      p2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(maxSeqNumber.toLong)
    }

    "delete some events for one persistenceId" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      val pid = nextPid()
      val p = spawn(Persister(pid))

      (1 to 8).foreach { n =>
        p ! Persister.PersistWithAck(n, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(p)

      val cleanup = new EventSourcedCleanup(system)
      cleanup.deleteEventsTo(pid, 5).futureValue

      val p2 = spawn(Persister(pid))
      p2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("6|7|8")
      p2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(8L)
    }

    "delete snapshots for one persistenceId" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val pid = nextPid()
      val p = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      (1 to 10).foreach { n =>
        p ! Persister.PersistWithAck(if (n == 3) s"$n-snap" else n, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(p)

      val cleanup = new EventSourcedCleanup(system)
      cleanup.deleteAllEvents(pid, resetSequenceNumber = false).futureValue

      val p2 = spawn(Persister(pid))
      p2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("1|2|3-snap")
      testKit.stop(p2)

      cleanup.deleteSnapshot(pid).futureValue

      val p3 = spawn(Persister(pid))
      p3 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("")
    }

    "cleanup before snapshot" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val pid = nextPid()
      val p = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      (1 to 10).foreach { n =>
        p ! Persister.PersistWithAck(if (n == 3) s"$n-snap" else n, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(p)

      val cleanup = new EventSourcedCleanup(system)
      cleanup.cleanupBeforeSnapshot(pid).futureValue

      val p2 = spawn(Persister(pid))
      p2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("1|2|3-snap|4|5|6|7|8|9|10")
      testKit.stop(p2)

      cleanup.deleteSnapshot(pid).futureValue

      val p3 = spawn(Persister(pid))
      p3 ! Persister.GetState(stateProbe.ref)
      // from replaying remaining events
      stateProbe.expectMessage("4|5|6|7|8|9|10")
    }

    "cleanup all before snapshot" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val pids = Vector(nextPid(), nextPid(), nextPid())
      val persisters =
        pids.map { pid =>
          spawn(Behaviors.setup[Persister.Command] { context =>
            Persister
              .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
              .snapshotWhen((_, event, _) => event.toString.contains("snap"))
          })
        }

      (1 to 10).foreach { n =>
        persisters.foreach { p =>
          p ! Persister.PersistWithAck(if (n == 3) s"$n-snap" else n, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(testKit.stop(_))

      val cleanup = new EventSourcedCleanup(system)
      cleanup.cleanupBeforeSnapshot(pids).futureValue
      cleanup.deleteSnapshots(pids).futureValue

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      persisters2.foreach { p =>
        p ! Persister.GetState(stateProbe.ref)
        // from replaying remaining events
        stateProbe.expectMessage("4|5|6|7|8|9|10")
      }
    }

    "delete all events and snapshots" in {
      val ackProbe = createTestProbe[Done]()
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      val pids = Vector(nextPid(), nextPid(), nextPid())
      val persisters =
        pids.map { pid =>
          spawn(Behaviors.setup[Persister.Command] { context =>
            Persister
              .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
              .snapshotWhen((_, event, _) => event.toString.contains("snap"))
          })
        }

      (1 to 10).foreach { n =>
        persisters.foreach { p =>
          p ! Persister.PersistWithAck(if (n == 3) s"$n-snap" else n, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(testKit.stop(_))

      val cleanup = new EventSourcedCleanup(system)
      cleanup.deleteAll(pids, resetSequenceNumber = true).futureValue

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      persisters2.foreach { p =>
        p ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage("")
        p ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(0L)
      }
    }

    // TODO: Delete before timestamp operations.
    //
    // "delete events for one persistenceId before timestamp" in {
    //   val ackProbe = createTestProbe[Done]()
    //   val pid = nextPid()
    //   val p = spawn(Persister(pid))
    //
    //   (1 to 10).foreach { n =>
    //     p ! Persister.PersistWithAck(n, ackProbe.ref)
    //     ackProbe.expectMessage(Done)
    //     ackProbe.expectNoMessage(1.millis) // just to be sure that events have different timestamps
    //   }
    //
    //   testKit.stop(p)
    //
    //   val journalQuery =
    //     PersistenceQuery(system).readJournalFor[CurrentEventsByPersistenceIdTypedQuery](DynamoDBReadJournal.Identifier)
    //   val eventsBefore =
    //     journalQuery.currentEventsByPersistenceIdTyped[Any](pid, 1L, Long.MaxValue).runWith(Sink.seq).futureValue
    //   eventsBefore.size shouldBe 10
    //
    //   val cleanup = new EventSourcedCleanup(system)
    //   val timestamp = eventsBefore.last.offset.asInstanceOf[TimestampOffset].timestamp
    //   cleanup.deleteEventsBefore(pid, timestamp).futureValue
    //
    //   val eventsAfter =
    //     journalQuery.currentEventsByPersistenceIdTyped[Any](pid, 1L, Long.MaxValue).runWith(Sink.seq).futureValue
    //   eventsAfter.size shouldBe 1
    //   eventsAfter.head.sequenceNr shouldBe eventsBefore.last.sequenceNr
    // }

    // TODO: Delete before timestamp operations.
    //
    // "delete events for slice before timestamp" in {
    //   val ackProbe = createTestProbe[Done]()
    //   val entityType = nextEntityType()
    //
    //   var (pid1, pid2) = pidsWithSliceLessThan256(entityType)
    //
    //   val p1 = spawn(Persister(pid1))
    //   val p2 = spawn(Persister(pid2))
    //
    //   (1 to 10).foreach { n =>
    //     val p = if (n % 2 == 0) p2 else p1
    //     p ! Persister.PersistWithAck(n, ackProbe.ref)
    //     ackProbe.expectMessage(Done)
    //     ackProbe.expectNoMessage(1.millis) // just to be sure that events have different timestamps
    //   }
    //
    //   testKit.stop(p1)
    //   testKit.stop(p2)
    //
    //   val journalQuery =
    //     PersistenceQuery(system).readJournalFor[CurrentEventsBySliceQuery](DynamoDBReadJournal.Identifier)
    //   val eventsBefore =
    //     journalQuery
    //       .currentEventsBySlices[Any](entityType, 0, 255, Offset.noOffset)
    //       .runWith(Sink.seq)
    //       .futureValue
    //   eventsBefore.size shouldBe 10
    //   eventsBefore.last.persistenceId shouldBe pid2.id
    //
    //   // we remove all except last for p2, and p1 should remain untouched
    //   val cleanup = new EventSourcedCleanup(system)
    //   val timestamp = eventsBefore.last.offset.asInstanceOf[TimestampOffset].timestamp
    //   val slice = persistenceExt.sliceForPersistenceId(eventsBefore.last.persistenceId)
    //   cleanup.deleteEventsBefore(entityType, slice, timestamp).futureValue
    //
    //   val eventsAfter =
    //     journalQuery
    //       .currentEventsBySlices[Any](entityType, 0, 255, Offset.noOffset)
    //       .runWith(Sink.seq)
    //       .futureValue
    //   eventsAfter.count(_.persistenceId == pid1.id) shouldBe 5
    //   eventsAfter.count(_.persistenceId == pid2.id) shouldBe 1
    //   eventsAfter.size shouldBe 5 + 1
    //   eventsAfter.filter(_.persistenceId == pid2.id).last.sequenceNr shouldBe eventsBefore.last.sequenceNr
    // }

    "set expiry for selected events for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 10
      val x = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForEvents(pid, toSequenceNr = x, expiryTimestamp).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < x) {
          eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else if (seqNr == x) { // expiry marker at x
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        } else {
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage(((x + 1) to n).mkString("|")) // expired events not included
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
    }

    "set expiry with time-to-live duration for selected events for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 10
      val x = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForEvents(pid, toSequenceNr = x, timeToLive).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < x) {
          eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else if (seqNr == x) { // expiry marker at x
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong should be >= beforeTimestamp.getEpochSecond
        } else {
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage((1 to n).mkString("|")) // no events have expired yet (1 minute TTL)
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
    }

    "set expiry for selected events for single persistence id in batches" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 300
      val x = 234

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)

      var iteration = 0
      val batchSize = 100 // hard-coded TransactWriteItems limit

      LoggingTestKit
        .debug("Updated expiry of events")
        .withOccurrences((x - 1) / 100 + 1)
        .withCustom { event =>
          val from = (iteration * batchSize) + 1
          iteration = iteration + 1
          val to = Math.min(x, from + batchSize - 1)
          val expectedMessage =
            s"Updated expiry of events for persistenceId [$pid], for sequence numbers [$from] to [$to]," +
            s" expiring at [$expiryTimestamp], consumed [4.0] WCU"
          event.message == expectedMessage
        }
        .expect {
          cleanup.setExpiryForEvents(pid, toSequenceNr = x, expiryTimestamp).futureValue
        }

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < x) {
          eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else if (seqNr == x) { // expiry marker at x
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        } else {
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage(((x + 1) to n).mkString("|")) // expired events not included
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
    }

    "set expiry for all events for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 10

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pid, resetSequenceNumber = true, expiryTimestamp).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        eventItem.get(ExpiryMarker) shouldBe None
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("") // all events have expired
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(0L) // no expiry marker (resetSequenceNumber = true)
    }

    "set expiry with time-to-live duration for all events for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 10

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pid, resetSequenceNumber = true, timeToLive).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
        eventItem.get(ExpiryMarker) shouldBe None
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage((1 to n).mkString("|")) // no events have expired yet (1 minute TTL)
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
    }

    "set expiry for all events for single persistence id in batches" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 321

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)

      var iteration = 0
      val batchSize = 100 // hard-coded TransactWriteItems limit

      LoggingTestKit
        .debug("Updated expiry of events")
        .withOccurrences((n - 1) / 100 + 1)
        .withCustom { event =>
          val from = (iteration * batchSize) + 1
          iteration = iteration + 1
          val to = Math.min(n, from + batchSize - 1)
          val expectedMessage =
            s"Updated expiry of events for persistenceId [$pid], for sequence numbers [$from] to [$to]," +
            s" expiring at [$expiryTimestamp], consumed [4.0] WCU"
          event.message == expectedMessage
        }
        .expect {
          cleanup.setExpiryForAllEvents(pid, resetSequenceNumber = true, expiryTimestamp).futureValue
        }

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        eventItem.get(ExpiryMarker) shouldBe None
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("") // all events have expired
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(0L) // no expiry marker (resetSequenceNumber = true)
    }

    "set expiry for all events for single persistence id and add expiry marker" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 10

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pid, resetSequenceNumber = false, expiryTimestamp).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < n) {
          eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else { // expiry marker for last event
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        }
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("") // all events have expired
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong) // from expiry marker (resetSequenceNumber = false)
    }

    "set expiry with time-to-live duration for all events for single persistence id and add expiry marker" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 10

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pid, resetSequenceNumber = false, timeToLive).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < n) {
          eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else { // expiry marker for last event
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong should be >= beforeTimestamp.getEpochSecond
        }
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage((1 to n).mkString("|")) // no events have expired yet (1 minute TTL)
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
    }

    "set expiry for all events for single persistence id in batches and add expiry marker" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()
      val persister = spawn(Persister(pid))
      val ackProbe = createTestProbe[Done]()

      val n = 321

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)

      var iteration = 0
      val batchSize = 100 // hard-coded TransactWriteItems limit

      LoggingTestKit
        .debug("Updated expiry of events")
        .withOccurrences((n - 1) / 100 + 1)
        .withCustom { event =>
          val from = (iteration * batchSize) + 1
          iteration = iteration + 1
          val to = Math.min(n, from + batchSize - 1)
          val expectedMessage =
            s"Updated expiry of events for persistenceId [$pid], for sequence numbers [$from] to [$to]," +
            s" expiring at [$expiryTimestamp], consumed [4.0] WCU"
          event.message == expectedMessage
        }
        .expect {
          cleanup.setExpiryForAllEvents(pid, resetSequenceNumber = false, expiryTimestamp).futureValue
        }

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < n) {
          eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else { // expiry marker for last event
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        }
      }

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("") // all events have expired
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong) // from expiry marker (resetSequenceNumber = false)
    }

    "set expiry for all events for multiple persistence ids" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pids = Seq(nextPid(), nextPid(), nextPid())
      val persisters = pids.map(pid => spawn(Persister(pid)))
      val ackProbe = createTestProbe[Done]()

      val n = 10

      (1 to n).foreach { i =>
        persisters.foreach { persister =>
          persister ! Persister.PersistWithAck(i, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(persister => testKit.stop(persister))

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pids, resetSequenceNumber = true, expiryTimestamp).futureValue

      pids.foreach { pid =>
        val eventItems = getEventItemsFor(pid)
        eventItems.size shouldBe n
        forAll(eventItems) { eventItem =>
          eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persisters2.foreach { persister2 =>
        persister2 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage("") // all events have expired
        persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(0L) // no expiry marker (resetSequenceNumber = true)
      }
    }

    "set expiry with time-to-live duration for all events for multiple persistence ids" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pids = Seq(nextPid(), nextPid(), nextPid())
      val persisters = pids.map(pid => spawn(Persister(pid)))
      val ackProbe = createTestProbe[Done]()

      val n = 10

      (1 to n).foreach { i =>
        persisters.foreach { persister =>
          persister ! Persister.PersistWithAck(i, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(persister => testKit.stop(persister))

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pids, resetSequenceNumber = true, timeToLive).futureValue

      pids.foreach { pid =>
        val eventItems = getEventItemsFor(pid)
        eventItems.size shouldBe n
        forAll(eventItems) { eventItem =>
          eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persisters2.foreach { persister2 =>
        persister2 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage((1 to n).mkString("|")) // no events have expired yet (1 minute TTL)
        persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(n.toLong)
      }
    }

    "set expiry for snapshot for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()

      val persister = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pid, resetSequenceNumber = false, expiryTimestamp).futureValue

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage((1 to s).mkString("|") + "-snap") // all events expired, state from snapshot
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
      testKit.stop(persister2)

      cleanup.setExpiryForSnapshot(pid, expiryTimestamp).futureValue

      val snapshotItem = getSnapshotItemFor(pid).value
      snapshotItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond

      val persister3 = spawn(Persister(pid))
      persister3 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("") // snapshot has expired
    }

    "set expiry with time-to-live duration for snapshot for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()

      val persister = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pid, resetSequenceNumber = false, expiryTimestamp).futureValue

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage((1 to s).mkString("|") + "-snap") // all events expired, state from snapshot
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
      testKit.stop(persister2)

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      cleanup.setExpiryForSnapshot(pid, timeToLive).futureValue

      val snapshotItem = getSnapshotItemFor(pid).value
      snapshotItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond

      val persister3 = spawn(Persister(pid))
      persister3 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage((1 to s).mkString("|") + "-snap") // snapshot not expired yet (1 minute TTL)
    }

    "set expiry for snapshot for multiple persistence ids" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pids = Seq(nextPid(), nextPid(), nextPid())

      val persisters = pids.map { pid =>
        spawn(Behaviors.setup[Persister.Command] { context =>
          Persister
            .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
            .snapshotWhen((_, event, _) => event.toString.contains("snap"))
        })
      }

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persisters.foreach { persister =>
          persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(persister => testKit.stop(persister))

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pids, resetSequenceNumber = false, expiryTimestamp).futureValue

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persisters2.foreach { persister2 =>
        persister2 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage((1 to s).mkString("|") + "-snap") // all events expired, state from snapshot
        persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(n.toLong)
      }

      cleanup.setExpiryForSnapshots(pids, expiryTimestamp).futureValue

      pids.foreach { pid =>
        val snapshotItem = getSnapshotItemFor(pid).value
        snapshotItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
      }

      val persisters3 = pids.map(pid => spawn(Persister(pid)))
      persisters3.foreach { persister3 =>
        persister3 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage("") // snapshot has expired
      }
    }

    "set expiry with time-to-live duration for snapshot for multiple persistence ids" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pids = Seq(nextPid(), nextPid(), nextPid())

      val persisters = pids.map { pid =>
        spawn(Behaviors.setup[Persister.Command] { context =>
          Persister
            .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
            .snapshotWhen((_, event, _) => event.toString.contains("snap"))
        })
      }

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persisters.foreach { persister =>
          persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(persister => testKit.stop(persister))

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEvents(pids, resetSequenceNumber = false, expiryTimestamp).futureValue

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persisters2.foreach { persister2 =>
        persister2 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage((1 to s).mkString("|") + "-snap") // all events expired, state from snapshot
        persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(n.toLong)
      }

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      cleanup.setExpiryForSnapshots(pids, timeToLive).futureValue

      pids.foreach { pid =>
        val snapshotItem = getSnapshotItemFor(pid).value
        snapshotItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
      }

      val persisters3 = pids.map(pid => spawn(Persister(pid)))
      persisters3.foreach { persister3 =>
        persister3 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage((1 to s).mkString("|") + "-snap") // snapshot not expired yet (1 minute TTL)
      }
    }

    "set expiry for events before snapshot for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()

      val persister = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForEventsBeforeSnapshot(pid, expiryTimestamp).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < s) {
          eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else if (seqNr == s) { // expiry marker at snapshot
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        } else {
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      val snapshotItem = getSnapshotItemFor(pid).value
      snapshotItem.get(Expiry) shouldBe None

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage((1 to n).map(i => if (i == s) s"$i-snap" else i).mkString("|"))
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
    }

    "set expiry with time-to-live duration for events before snapshot for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()

      val persister = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForEventsBeforeSnapshot(pid, timeToLive).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < s) {
          eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else if (seqNr == s) { // expiry marker at snapshot
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong should be >= beforeTimestamp.getEpochSecond
        } else {
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker) shouldBe None
        }
      }

      val snapshotItem = getSnapshotItemFor(pid).value
      snapshotItem.get(Expiry) shouldBe None

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage((1 to n).map(i => if (i == s) s"$i-snap" else i).mkString("|"))
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong)
    }

    "set expiry for events before snapshots for multiple persistence ids" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pids = Seq(nextPid(), nextPid(), nextPid())

      val persisters = pids.map { pid =>
        spawn(Behaviors.setup[Persister.Command] { context =>
          Persister
            .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
            .snapshotWhen((_, event, _) => event.toString.contains("snap"))
        })
      }

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persisters.foreach { persister =>
          persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(persister => testKit.stop(persister))

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForEventsBeforeSnapshots(pids, expiryTimestamp).futureValue

      pids.foreach { pid =>
        val eventItems = getEventItemsFor(pid)
        eventItems.size shouldBe n
        forAll(eventItems) { eventItem =>
          val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
          if (seqNr < s) {
            eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
            eventItem.get(ExpiryMarker) shouldBe None
          } else if (seqNr == s) { // expiry marker at snapshot
            eventItem.get(Expiry) shouldBe None
            eventItem.get(ExpiryMarker).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          } else {
            eventItem.get(Expiry) shouldBe None
            eventItem.get(ExpiryMarker) shouldBe None
          }
        }

        val snapshotItem = getSnapshotItemFor(pid).value
        snapshotItem.get(Expiry) shouldBe None
      }

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persisters2.foreach { persister2 =>
        persister2 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage((1 to n).map(i => if (i == s) s"$i-snap" else i).mkString("|"))
        persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(n.toLong)
      }
    }

    "set expiry with time-to-live duration for events before snapshots for multiple persistence ids" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pids = Seq(nextPid(), nextPid(), nextPid())

      val persisters = pids.map { pid =>
        spawn(Behaviors.setup[Persister.Command] { context =>
          Persister
            .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
            .snapshotWhen((_, event, _) => event.toString.contains("snap"))
        })
      }

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persisters.foreach { persister =>
          persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(persister => testKit.stop(persister))

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForEventsBeforeSnapshots(pids, timeToLive).futureValue

      pids.foreach { pid =>
        val eventItems = getEventItemsFor(pid)
        eventItems.size shouldBe n
        forAll(eventItems) { eventItem =>
          val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
          if (seqNr < s) {
            eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
            eventItem.get(ExpiryMarker) shouldBe None
          } else if (seqNr == s) { // expiry marker at snapshot
            eventItem.get(Expiry) shouldBe None
            eventItem.get(ExpiryMarker).value.n.toLong should be >= beforeTimestamp.getEpochSecond
          } else {
            eventItem.get(Expiry) shouldBe None
            eventItem.get(ExpiryMarker) shouldBe None
          }
        }

        val snapshotItem = getSnapshotItemFor(pid).value
        snapshotItem.get(Expiry) shouldBe None
      }

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persisters2.foreach { persister2 =>
        persister2 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage((1 to n).map(i => if (i == s) s"$i-snap" else i).mkString("|"))
        persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(n.toLong)
      }
    }

    "set expiry for all events and snapshot for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()

      val persister = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEventsAndSnapshot(pid, resetSequenceNumber = true, expiryTimestamp).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        eventItem.get(ExpiryMarker) shouldBe None
      }

      val snapshotItem = getSnapshotItemFor(pid).value
      snapshotItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("") // all events and snapshot expired
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(0L) // no expiry marker (resetSequenceNumber = true)
    }

    "set expiry with time-to-live duration for all events and snapshot for single persistence id" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()

      val persister = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEventsAndSnapshot(pid, resetSequenceNumber = true, timeToLive).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
        eventItem.get(ExpiryMarker) shouldBe None
      }

      val snapshotItem = getSnapshotItemFor(pid).value
      snapshotItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      // events and snapshot not expired yet (1 minute TTL)
      stateProbe.expectMessage((1 to n).map(i => if (i == s) s"$i-snap" else i).mkString("|"))
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong) // not expired yet
    }

    "set expiry for all events and snapshot for single persistence id and add expiry marker" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()

      val persister = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEventsAndSnapshot(pid, resetSequenceNumber = false, expiryTimestamp).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < n) {
          eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else { // expiry marker for last event
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
        }
      }

      val snapshotItem = getSnapshotItemFor(pid).value
      snapshotItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("") // all events and snapshot expired
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong) // from expiry marker (resetSequenceNumber = false)
    }

    "set expiry with time-to-live duration for all events and snapshot for single persistence id and add expiry marker" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pid = nextPid()

      val persister = spawn(Behaviors.setup[Persister.Command] { context =>
        Persister
          .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
          .snapshotWhen((_, event, _) => event.toString.contains("snap"))
      })

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
        ackProbe.expectMessage(Done)
      }

      testKit.stop(persister)

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEventsAndSnapshot(pid, resetSequenceNumber = false, timeToLive).futureValue

      val eventItems = getEventItemsFor(pid)
      eventItems.size shouldBe n
      forAll(eventItems) { eventItem =>
        val seqNr = eventItem.get(SeqNr).fold(0L)(_.n.toLong)
        if (seqNr < n) {
          eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        } else { // expiry marker for last event
          eventItem.get(Expiry) shouldBe None
          eventItem.get(ExpiryMarker).value.n.toLong should be >= beforeTimestamp.getEpochSecond
        }
      }

      val snapshotItem = getSnapshotItemFor(pid).value
      snapshotItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond

      val persister2 = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persister2 ! Persister.GetState(stateProbe.ref)
      // events and snapshot not expired yet (1 minute TTL)
      stateProbe.expectMessage((1 to n).map(i => if (i == s) s"$i-snap" else i).mkString("|"))
      persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
      seqNrProbe.expectMessage(n.toLong) // not expired yet
    }

    "set expiry for all events and snapshots for multiple persistence ids" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pids = Seq(nextPid(), nextPid(), nextPid())

      val persisters = pids.map { pid =>
        spawn(Behaviors.setup[Persister.Command] { context =>
          Persister
            .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
            .snapshotWhen((_, event, _) => event.toString.contains("snap"))
        })
      }

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persisters.foreach { persister =>
          persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(persister => testKit.stop(persister))

      val expiryTimestamp = Instant.now().minusSeconds(1) // already expired

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEventsAndSnapshots(pids, resetSequenceNumber = true, expiryTimestamp).futureValue

      pids.foreach { pid =>
        val eventItems = getEventItemsFor(pid)
        eventItems.size shouldBe n
        forAll(eventItems) { eventItem =>
          eventItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        }

        val snapshotItem = getSnapshotItemFor(pid).value
        snapshotItem.get(Expiry).value.n.toLong shouldBe expiryTimestamp.getEpochSecond
      }

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persisters2.foreach { persister2 =>
        persister2 ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage("") // all events and snapshot expired
        persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(0L) // no expiry marker (resetSequenceNumber = true)
      }
    }

    "set expiry with time-to-live duration for all events and snapshots for multiple persistence ids" in {
      import akka.persistence.dynamodb.internal.JournalAttributes._

      val pids = Seq(nextPid(), nextPid(), nextPid())

      val persisters = pids.map { pid =>
        spawn(Behaviors.setup[Persister.Command] { context =>
          Persister
            .eventSourcedBehavior(PersistenceId.ofUniqueId(pid), context)
            .snapshotWhen((_, event, _) => event.toString.contains("snap"))
        })
      }

      val ackProbe = createTestProbe[Done]()

      val n = 10
      val s = 5

      (1 to n).foreach { i =>
        persisters.foreach { persister =>
          persister ! Persister.PersistWithAck(if (i == s) s"$i-snap" else i, ackProbe.ref)
          ackProbe.expectMessage(Done)
        }
      }

      persisters.foreach(persister => testKit.stop(persister))

      val timeToLive = 1.minute
      val beforeTimestamp = Instant.now().plusSeconds(timeToLive.toSeconds) // same second or before

      val cleanup = new EventSourcedCleanup(system)
      cleanup.setExpiryForAllEventsAndSnapshots(pids, resetSequenceNumber = true, timeToLive).futureValue

      pids.foreach { pid =>
        val eventItems = getEventItemsFor(pid)
        eventItems.size shouldBe n
        forAll(eventItems) { eventItem =>
          eventItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
          eventItem.get(ExpiryMarker) shouldBe None
        }

        val snapshotItem = getSnapshotItemFor(pid).value
        snapshotItem.get(Expiry).value.n.toLong should be >= beforeTimestamp.getEpochSecond
      }

      val persisters2 = pids.map(pid => spawn(Persister(pid)))
      val stateProbe = createTestProbe[String]()
      val seqNrProbe = createTestProbe[Long]()
      persisters2.foreach { persister2 =>
        persister2 ! Persister.GetState(stateProbe.ref)
        // events and snapshot not expired yet (1 minute TTL)
        stateProbe.expectMessage((1 to n).map(i => if (i == s) s"$i-snap" else i).mkString("|"))
        persister2 ! Persister.GetSeqNr(seqNrProbe.ref)
        seqNrProbe.expectMessage(n.toLong) // not expired yet
      }
    }

  }
}
