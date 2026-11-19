/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal.query

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import akka.persistence.query.scaladsl.ReadJournal
import com.typesafe.config.Config

final class MultiJournalReadJournalProvider(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournalProvider {
  private val scaladslInstance = new scaladsl.MultiJournalReadJournal(system, config, cfgPath)
  private val javadslInstance = new javadsl.MultiJournalReadJournal(scaladslInstance)

  override def scaladslReadJournal(): ReadJournal = scaladslInstance

  override def javadslReadJournal(): javadsl.MultiJournalReadJournal = javadslInstance
}
