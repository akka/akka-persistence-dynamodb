/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.journal

import java.time.temporal.ChronoUnit

import akka.actor.testkit.typed.scaladsl.{ LogCapturing, LoggingTestKit, ScalaTestWithActorTestKit }
import akka.actor.typed.ActorSystem
import akka.persistence.JournalProtocol.{ RecoverySuccess, ReplayMessages, ReplayedMessage }
import akka.persistence.dynamodb.internal.InstantFactory
import akka.persistence.dynamodb.query.EventsBySliceSpec
import akka.persistence.dynamodb.{ TestData, TestDbLifecycle }
import akka.testkit.TestProbe
import org.scalatest.wordspec.AnyWordSpecLike

class ClockSkewDetectionSpec
    extends ScalaTestWithActorTestKit(EventsBySliceSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val journal = persistenceExt.journalFor(null)

  "DynamoDBJournal" should {

    "detect clock skew on event replay" in {
      val entityType = nextEntityType()
      val pid = nextPersistenceId(entityType)
      val slice = persistenceExt.sliceForPersistenceId(pid.id)

      val now = InstantFactory.now().truncatedTo(ChronoUnit.SECONDS)

      // first 5 events in the past
      for (n <- 1 to 5) {
        writeEvent(slice, pid, n, now.minusSeconds(10).plusSeconds(n), s"e$n")
      }

      // next 5 events over 1 minute in the future
      for (n <- 6 to 10) {
        writeEvent(slice, pid, n, now.plusSeconds(60).plusSeconds(n), s"e$n")
      }

      val replayProbe = TestProbe()(system.classicSystem)

      LoggingTestKit
        .warn("Detected clock skew when replaying events:" +
        s" persistence id [${pid.id}], highest seq nr [10] written at [${now.plusSeconds(70)}]")
        .expect {
          journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid.id, replayProbe.ref)
          (1 to 10).foreach { _ => replayProbe.expectMsgType[ReplayedMessage] }
          replayProbe.expectMsg(RecoverySuccess(highestSequenceNr = 10L))
        }
    }

  }
}
