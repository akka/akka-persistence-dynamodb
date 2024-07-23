/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.cleanup

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
          val deleted = to - from + 1
          val expectedMsg = s"Deleted events from [$from] to [$to] for persistenceId [$pid], consumed [8.0] WCU"
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
          val deleted = to - from + 1
          val expectedMsg = s"Deleted events from [$from] to [$to] for persistenceId [$pid], consumed [8.0] WCU"
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
        p ! Persister.PersistWithAck(s"${if (n == 3) n + "-snap" else n}", ackProbe.ref)
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
        p ! Persister.PersistWithAck(s"${if (n == 3) n + "-snap" else n}", ackProbe.ref)
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
          p ! Persister.PersistWithAck(s"${if (n == 3) n + "-snap" else n}", ackProbe.ref)
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
          p ! Persister.PersistWithAck(s"${if (n == 3) n + "-snap" else n}", ackProbe.ref)
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

  }
}
