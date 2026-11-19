/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import akka.testkit.{ TestKit => ClassicTestKit }
import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.Persistence
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object PluginLoadSpec {
  val MJPath = "akka.persistence.multi-journal"
  val noJournalsConfig = ConfigFactory.load()
  val journalWithNonIntKey = ConfigFactory
    .parseString(s"""$MJPath.journals.foo = "akka.persistence.dynamodb.journal.DynamoDBJournal"""")
    .withFallback(noJournalsConfig)

  val journalWithNegativeKey = ConfigFactory
    .parseString(s"""$MJPath.journals."-1" = "akka.persistence.dynamodb.journal.DynamoDBJournal"""")
    .withFallback(noJournalsConfig)

  val journalWithNonexistentPath =
    ConfigFactory.parseString(s"""$MJPath.journals.0 = "some.nonexistent.path"""").withFallback(noJournalsConfig)

  val journalWithPathThatsNotAJournal =
    ConfigFactory.parseString(s"""$MJPath.journals.0 = "akka.scheduler"""").withFallback(noJournalsConfig)

  val journalWithUnsupportedJournal =
    ConfigFactory
      .parseString(s"""
      $MJPath.journals.0 = "akka.persistence.multi-journal"""")
      .withFallback(noJournalsConfig)
}

class PluginLoadSpec extends ScalaTestWithActorTestKit(PluginLoadSpec.noJournalsConfig) with AnyWordSpecLike {
  import PluginLoadSpec._

  "A multi-journal" must {
    "fail fast if no journals are configured" in {
      val tk = new ClassicTestKit(system.classicSystem)
      val journal = Persistence(system).journalFor("akka.persistence.multi-journal")
      tk.watch(journal)
      tk.expectTerminated(journal)
    }

    Map(
      "non-integer-key" -> journalWithNonIntKey,
      "negative-key" -> journalWithNegativeKey,
      "nonexistent-path" -> journalWithNonexistentPath,
      "journal-is-not-a-journal" -> journalWithPathThatsNotAJournal,
      "unsupported-journal" -> journalWithUnsupportedJournal)
      .foreach { case (whyBad, badConfig) =>
        s"fail fast if an improper configuration is used: $whyBad" in {
          val sys = ActorSystem(s"${system.name}-$whyBad", badConfig)
          val tk = new ClassicTestKit(sys)
          try {
            val journal = Persistence(sys).journalFor("akka.persistence.multi-journal")
            tk.watch(journal)
            tk.expectTerminated(journal)
          } finally {
            ClassicTestKit.shutdownActorSystem(sys)
          }
        }
      }
  }
}
