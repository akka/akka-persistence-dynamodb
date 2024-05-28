/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.journal

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.CapabilityFlag
import akka.persistence.JournalProtocol.RecoverySuccess
import akka.persistence.JournalProtocol.ReplayMessages
import akka.persistence.dynamodb.TestConfig
import akka.persistence.dynamodb.TestDbLifecycle
import akka.persistence.journal.JournalSpec
import akka.testkit.TestProbe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually

object DynamoDBJournalSpec {
  val config = DynamoDBJournalSpec.testConfig()

  def configWithMeta =
    ConfigFactory
      .parseString("""akka.persistence.dynamodb.with-meta = true""")
      .withFallback(DynamoDBJournalSpec.testConfig())

  def testConfig(): Config = {
    ConfigFactory
      .parseString(s"""
      # allow java serialization when testing
      akka.actor.allow-java-serialization = on
      akka.actor.warn-about-java-serializer-usage = off
      """)
      .withFallback(TestConfig.config)
  }
}

abstract class DynamoDBJournalBaseSpec(config: Config)
    extends JournalSpec(config)
    with TestDbLifecycle
    with Eventually {

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.off()

  override def typedSystem: ActorSystem[_] = system.toTyped

  // always eventually consistent reads with dynamodb-local, wait for writes to be seen
  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
    val probe = TestProbe()
    eventually(timeout(3.seconds), interval(100.millis)) {
      journal ! ReplayMessages(fromSnr, toSnr, max = 0, pid, probe.ref)
      probe.expectMsg(RecoverySuccess(toSnr))
    }
  }
}

class DynamoDBJournalSpec extends DynamoDBJournalBaseSpec(DynamoDBJournalSpec.config)

class DynamoDBJournalWithMetaSpec extends DynamoDBJournalBaseSpec(DynamoDBJournalSpec.configWithMeta) {
  protected override def supportsMetadata: CapabilityFlag = CapabilityFlag.on()
}
