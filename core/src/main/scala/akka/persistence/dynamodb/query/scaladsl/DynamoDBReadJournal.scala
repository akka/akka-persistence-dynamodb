/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.persistence.dynamodb.ClientProvider
import akka.persistence.dynamodb.DynamoDBSettings
import akka.persistence.dynamodb.internal.QueryDao
import akka.persistence.dynamodb.internal.SerializedJournalItem
import akka.persistence.query.scaladsl._
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object DynamoDBReadJournal {
  val Identifier = "akka.persistence.dynamodb.query"
}

final class DynamoDBReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String) extends ReadJournal {

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = DynamoDBSettings(system.settings.config.getConfig(sharedConfigPath))
  log.debug("DynamoDB read journal starting up")

  private val typedSystem = system.toTyped

  private val client = ClientProvider(typedSystem).clientFor(sharedConfigPath + ".client")
  private val queryDao = new QueryDao(typedSystem, settings, client)

  /**
   * INTERNAL API: Used by both journal replay and currentEventsByPersistenceId
   */
  @InternalApi private[dynamodb] def internalCurrentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalItem, NotUsed] = {

    queryDao.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
  }

}
