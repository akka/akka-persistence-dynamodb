/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.query

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import akka.persistence.query.scaladsl.ReadJournal
import com.typesafe.config.Config

final class DynamoDBReadJournalProvider(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournalProvider {

  private val scaladslReadJournalInstance =
    new scaladsl.DynamoDBReadJournal(system, config, cfgPath)

  override def scaladslReadJournal(): ReadJournal = scaladslReadJournalInstance

  private val javadslReadJournalInstance =
    new javadsl.DynamoDBReadJournal(scaladslReadJournalInstance)

  override def javadslReadJournal(): javadsl.DynamoDBReadJournal = javadslReadJournalInstance

}
