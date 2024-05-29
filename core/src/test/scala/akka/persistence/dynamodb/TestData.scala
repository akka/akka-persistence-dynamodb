/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec

import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.typed.PersistenceId

object TestData {
  private val start = 0L // could be something more unique, like currentTimeMillis
  private val pidCounter = new AtomicLong(start)
  private val entityTypeCounter = new AtomicLong(start)
}

trait TestData {
  import TestData.pidCounter
  import TestData.entityTypeCounter

  def typedSystem: ActorSystem[_]

  private lazy val persistenceExt = Persistence(typedSystem)

  def nextPid(): String = s"p-${pidCounter.incrementAndGet()}"

  def nextEntityType(): String = s"TestEntity-${entityTypeCounter.incrementAndGet()}"

  def nextPersistenceId(entityType: String): PersistenceId =
    PersistenceId.of(entityType, s"${pidCounter.incrementAndGet()}")

  @tailrec final def randomPersistenceIdForSlice(entityType: String, slice: Int): PersistenceId = {
    val p = PersistenceId.of(entityType, UUID.randomUUID().toString)
    if (persistenceExt.sliceForPersistenceId(p.id) == slice) p
    else randomPersistenceIdForSlice(entityType, slice)
  }

}
