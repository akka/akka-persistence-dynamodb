/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import java.util.concurrent.atomic.AtomicLong

import akka.persistence.typed.PersistenceId

object TestData {
  private val start = 0L // could be something more unique, like currentTimeMillis
  private val pidCounter = new AtomicLong(start)
  private val entityTypeCounter = new AtomicLong(start)
}

trait TestData {
  import TestData.pidCounter
  import TestData.entityTypeCounter

  def nextPid(): String = s"p-${pidCounter.incrementAndGet()}"

  def nextEntityType(): String = s"TestEntity-${entityTypeCounter.incrementAndGet()}"

  def nextPersistenceId(entityType: String): PersistenceId =
    PersistenceId.of(entityType, s"${pidCounter.incrementAndGet()}")

}
