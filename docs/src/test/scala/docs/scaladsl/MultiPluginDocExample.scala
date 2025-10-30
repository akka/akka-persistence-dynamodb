/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestData
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object MultiPluginDocExample {
  val config: Config = ConfigFactory
    .parseString(s"""
    //#default-config
    akka.persistence.journal.plugin = "akka.persistence.dynamodb.journal"
    akka.persistence.snapshot-store.plugin = "akka.persistence.dynamodb.snapshot"
    //#default-config

    //#second-config
    second-dynamodb = $${akka.persistence.dynamodb}
    second-dynamodb {
      client {
        # specific client settings here
      }
      journal {
        # specific journal settings here
      }
      snapshot {
        # specific snapshot settings here
      }
      query {
        # specific query settings here
      }
    }
    //#second-config
    """)
    .withFallback(TestConfig.config)
    .resolve()

  object MyEntity {
    sealed trait Command
    final case class Persist(payload: String, replyTo: ActorRef[State]) extends Command
    type Event = String
    object State {
      def apply(): State = ""
    }
    type State = String

    def commandHandler(state: State, cmd: Command): Effect[Event, State] = {
      cmd match {
        case Persist(payload, replyTo) =>
          Effect
            .persist(payload)
            .thenReply(replyTo)(newState => newState)
      }
    }

    def eventHandler(state: State, evt: Event): State =
      state + evt

    def apply(persistenceId: PersistenceId): EventSourcedBehavior[Command, Event, State] = {
      //#with-plugins
      EventSourcedBehavior(persistenceId, emptyState = State(), commandHandler, eventHandler)
        .withJournalPluginId("second-dynamodb.journal")
        .withSnapshotPluginId("second-dynamodb.snapshot")
      //#with-plugins
    }
  }
}

class MultiPluginDocExample
    extends ScalaTestWithActorTestKit(MultiPluginDocExample.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  import MultiPluginDocExample.MyEntity

  override def typedSystem: ActorSystem[_] = system

  "Addition DynamoDB plugin config" should {

    "be supported for EventSourcedBehavior" in {
      val probe = createTestProbe[MyEntity.State]()
      val pid = PersistenceId.ofUniqueId(nextPid())
      val ref = spawn(MyEntity(pid))
      ref ! MyEntity.Persist("a", probe.ref)
      probe.expectMessage("a")
      ref ! MyEntity.Persist("b", probe.ref)
      probe.expectMessage("ab")
    }

  }
}
