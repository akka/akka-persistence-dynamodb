/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.temporal.ChronoUnit

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InstantFactorySpec extends AnyWordSpec with TestSuite with Matchers {
  "InstantFactory" should {
    "truncate now to micros" in {
      InstantFactory.now().getNano % 1000 shouldBe 0
    }

    "always increasing or equal now" in {
      val now1 = InstantFactory.now()
      val now2 = InstantFactory.now()
      Thread.sleep(1)
      val now3 = InstantFactory.now()
      now2.isBefore(now1) shouldBe false
      now3.isBefore(now2) shouldBe false
      now3.isBefore(now1) shouldBe false
    }

    "convert to/from micros since epoch" in {
      import InstantFactory._
      val now1 = now()
      fromEpochMicros(toEpochMicros(now1)) shouldBe now1
    }

    "should not overflow micros since epoch" in {
      import InstantFactory._
      (10 to 1000).foreach { years =>
        val future = now().plus(years * 365, ChronoUnit.DAYS)
        toEpochMicros(future) shouldBe <(Long.MaxValue)
        fromEpochMicros(toEpochMicros(future)) shouldBe future
      }
    }
  }

}
