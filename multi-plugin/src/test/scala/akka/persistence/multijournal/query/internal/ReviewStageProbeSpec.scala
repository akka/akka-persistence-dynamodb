/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal.query.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.typed.EventEnvelope
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike

class ReviewStageProbeSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  "MergeHotWithCoolerStage" should {
    "F4: refresh on wall-clock time while the hot journal is idle" in {
      val coolerQueries = new AtomicInteger

      val (killSwitch, _) = Source
        .fromGraph(
          new MergeHotWithCoolerStage[String](
            Source.maybe[EventEnvelope[String]].mapMaterializedValue(_ => NotUsed),
            _ => {
              coolerQueries.incrementAndGet()
              Source.empty
            },
            None,
            200.millis,
            50.millis))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

      // threshold is 125ms, so 3s is 24 thresholds
      Thread.sleep(3000)
      killSwitch.shutdown()
      info(s"cooler queries in 3s: ${coolerQueries.get}")
      coolerQueries.get should be >= 2
    }
  }
}
