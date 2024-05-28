/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object InstantFactory {
  private val previousNow = new AtomicReference(Instant.EPOCH)

  /**
   * Current time truncated to microseconds. Within this JVM it's guaranteed to be equal to or greater than previous
   * invocation of `now`.
   */
  def now(): Instant = {
    val n = Instant.now().truncatedTo(ChronoUnit.MICROS)
    previousNow.updateAndGet { prev =>
      if (prev.isAfter(n)) prev.plus(1, ChronoUnit.MICROS)
      else n
    }
  }

  def toEpochMicros(instant: Instant): Long =
    instant.getEpochSecond * 1_000_000 + (instant.getNano / 1000)

  def fromEpochMicros(micros: Long): Instant = {
    val epochSeconds = micros / 1_000_000
    val nanos = (micros % 1_000_000) * 1000
    Instant.ofEpochSecond(epochSeconds, nanos)
  }

}
