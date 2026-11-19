/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import scala.util.Success

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object ReadTheWritesSpec {
  val Config: Config = ConfigFactory
    .parseString(s"""
    |two-journal = $${akka.persistence.multi-journal}
    |two-journal.journals {
    |  0 = "cold-journal"
    |  1 = "lukewarm-journal"
    |}
    |
    |three-journal = $${akka.persistence.multi-journal}
    |three-journal.journals {
    |  0 = "cold-journal"
    |  1 = "lukewarm-journal"
    |  2 = "hot-journal"
    |}
    |""".stripMargin)
    .withFallback(ReplaySpec.Journals)
    .withFallback(ConfigFactory.load())
    .resolve()

  object TestActor {
    sealed trait Command
    final case class Persist(events: Seq[String], replyTo: ActorRef[Vector[String]]) extends Command
    final case class GetState(replyTo: ActorRef[Vector[String]]) extends Command

    def apply(pid: String, journalPluginId: String): Behavior[Command] =
      EventSourcedBehavior[Command, String, Vector[String]](
        PersistenceId.ofUniqueId(pid),
        Vector.empty,
        (state, command) =>
          command match {
            case Persist(events, replyTo) => Effect.persist(events).thenReply(replyTo)(identity)
            case GetState(replyTo)        => Effect.reply(replyTo)(state)
          },
        (state, event) => state :+ event).withJournalPluginId(journalPluginId)
  }

  final class InMemoryJournal(val probe: akka.actor.testkit.typed.scaladsl.TestProbe[TestProbeJournal.Invocation]) {
    private var storedEvents = Vector.empty[PersistentRepr]

    def events: Vector[PersistentRepr] = storedEvents

    def serviceNextInvocation(): TestProbeJournal.Invocation = {
      val invocation = probe.receiveMessage()
      invocation match {
        case read: TestProbeJournal.ReadHighestSeqNr =>
          val highest = storedEvents.iterator
            .filter(_.persistenceId == read.pid)
            .map(_.sequenceNr)
            .maxOption
            .getOrElse(0L)
          read.promise.success(highest)

        case replay: TestProbeJournal.ReplayMessages =>
          val inRange = storedEvents.iterator
            .filter(_.persistenceId == replay.pid)
            .filter(event => event.sequenceNr >= replay.fromSequenceNr && event.sequenceNr <= replay.toSequenceNr)
            .toVector
          val selected =
            if (replay.fromSequenceNr == -1) inRange.lastOption.toSeq
            else inRange.take(replay.max.min(Int.MaxValue).toInt)
          replay.promise.success(selected)

        case write: TestProbeJournal.WriteMessages =>
          storedEvents ++= write.messages.iterator.flatMap(_.payload)
          write.promise.success(Seq.fill(write.messages.size)(Success(())))

        case unexpected =>
          throw new IllegalStateException(s"Unexpected journal invocation: $unexpected")
      }
      invocation
    }

    def serviceInvocations(count: Int): Unit =
      (1 to count).foreach(_ => serviceNextInvocation())
  }
}

class ReadTheWritesSpec
    extends ScalaTestWithActorTestKit(ReadTheWritesSpec.Config)
    with AnyWordSpecLike
    with LogCapturing
    with TestData {
  import ReadTheWritesSpec._
  import TestActor._

  private implicit val classicSystem: akka.actor.ActorSystem = system.classicSystem
  val initializedJournals = Seq("cold-journal", "two-journal", "three-journal").map { pluginId =>
    Persistence(classicSystem).journalFor(pluginId)
  }
  private val cold = new InMemoryJournal(TestProbeJournal.getProbe("replay-spec-cold-journal"))
  private val lukewarm = new InMemoryJournal(TestProbeJournal.getProbe("replay-spec-lukewarm-journal"))
  private val hot = new InMemoryJournal(TestProbeJournal.getProbe("replay-spec-hot-journal"))

  "MultiJournal" should {
    "read events written before and during journal migrations" in {
      val pid = nextPid()
      val stateProbe = createTestProbe[Vector[String]]()

      val beforeMigration = spawn(TestActor(pid, "cold-journal"))
      beforeMigration ! GetState(stateProbe.ref)
      cold.serviceInvocations(1)
      stateProbe.expectMessage(Vector.empty)

      beforeMigration ! Persist(Seq("cold-1", "cold-2"), stateProbe.ref)
      cold.serviceInvocations(1)
      stateProbe.expectMessage(Vector("cold-1", "cold-2"))
      cold.events.map(_.metadata) should contain only None
      testKit.stop(beforeMigration)

      val withLukewarm = spawn(TestActor(pid, "two-journal"))
      withLukewarm ! GetState(stateProbe.ref)
      lukewarm.serviceInvocations(1)
      cold.serviceInvocations(2)
      stateProbe.expectMessage(Vector("cold-1", "cold-2"))

      withLukewarm ! Persist(Seq("lukewarm-3", "lukewarm-4"), stateProbe.ref)
      lukewarm.serviceInvocations(1)
      stateProbe.expectMessage(Vector("cold-1", "cold-2", "lukewarm-3", "lukewarm-4"))
      lukewarm.events.map(_.metadata) should contain only Some(MultiJournalMetadata(Map(0 -> 2L)))
      testKit.stop(withLukewarm)

      val withHot = spawn(TestActor(pid, "three-journal"))
      withHot ! GetState(stateProbe.ref)
      hot.serviceInvocations(1)
      lukewarm.serviceInvocations(2)
      cold.serviceInvocations(2)
      stateProbe.expectMessage(Vector("cold-1", "cold-2", "lukewarm-3", "lukewarm-4"))

      withHot ! Persist(Seq("hot-5", "hot-6"), stateProbe.ref)
      hot.serviceInvocations(1)
      val allEvents = Vector("cold-1", "cold-2", "lukewarm-3", "lukewarm-4", "hot-5", "hot-6")
      stateProbe.expectMessage(allEvents)
      hot.events.map(_.metadata) should contain only Some(MultiJournalMetadata(Map(0 -> 2L, 1 -> 4L)))
      testKit.stop(withHot)

      val restarted = spawn(TestActor(pid, "three-journal"))
      restarted ! GetState(stateProbe.ref)
      hot.serviceInvocations(2)
      lukewarm.serviceInvocations(2)
      cold.serviceInvocations(2)
      stateProbe.expectMessage(allEvents)
      testKit.stop(restarted)
    }
  }
}
