/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.query.javadsl

import akka.persistence.dynamodb.query.scaladsl
import akka.persistence.query.javadsl._

object DynamodbReadJournal {
  val Identifier: String = scaladsl.DynamoDBReadJournal.Identifier
}

final class DynamodbReadJournal(delegate: scaladsl.DynamoDBReadJournal) extends ReadJournal {}
