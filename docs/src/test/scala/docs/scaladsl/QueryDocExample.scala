/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

//#access-read-journal
import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import akka.persistence.query.PersistenceQuery

//#access-read-journal

//#current-events-by-slices
import akka.persistence.query.NoOffset
import akka.stream.scaladsl.Sink

//#current-events-by-slices

object QueryDocExample {
  val config: Config = ConfigFactory.load(ConfigFactory.parseString("""
    akka.persistence.dynamodb {
      client.local.enabled = true
    }
    """))

  trait MyEvent
}

class QueryDocExample extends ScalaTestWithActorTestKit(QueryDocExample.config) with AnyWordSpecLike {
  import QueryDocExample._

  def accessReadJournal: DynamoDBReadJournal = {
    //#access-read-journal
    val eventQueries = PersistenceQuery(system)
      .readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)
    //#access-read-journal
    eventQueries
  }

  def currentEventsBySliceExampleCompileOnly(): Unit = {
    val eventQueries = accessReadJournal

    //#current-events-by-slices
    // Split the slices into 4 ranges
    val numberOfSliceRanges: Int = 4
    val sliceRanges = eventQueries.sliceRanges(numberOfSliceRanges)

    // Example of using the first slice range
    val minSlice: Int = sliceRanges.head.min
    val maxSlice: Int = sliceRanges.head.max
    val entityType: String = "MyEntity"
    eventQueries
      .currentEventsBySlices[MyEvent](entityType, minSlice, maxSlice, NoOffset)
      .map(envelope =>
        s"event from persistenceId ${envelope.persistenceId} with " +
        s"seqNr ${envelope.sequenceNr}: ${envelope.event}")
      .runWith(Sink.foreach(println))
    //#current-events-by-slices
  }

  "Query plugin docs" should {

    "have example of accessing read journal (Scala)" in {
      accessReadJournal shouldBe a[DynamoDBReadJournal]
    }

    "have example of accessing read journal (Java)" in {
      val readJournal = docs.javadsl.QueryDocExample.accessReadJournal(system)
      readJournal shouldBe an[akka.persistence.dynamodb.query.javadsl.DynamoDBReadJournal]
    }

  }
}
