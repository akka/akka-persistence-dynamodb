/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import java.util.concurrent.atomic.AtomicLong

object TestData {
  private val pidCounter = new AtomicLong(0)
}

trait TestData {
  import TestData.pidCounter

  def nextPid(): String = s"p-${pidCounter.incrementAndGet()}"
}
