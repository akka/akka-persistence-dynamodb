/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object InstantFactory {

  /**
   * Current time truncated to microseconds.
   */
  def now(): Instant =
    Instant.now().truncatedTo(ChronoUnit.MICROS)

}
