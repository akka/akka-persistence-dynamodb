/*
 * Copyright (C) 2024-2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.multijournal

import scala.reflect.ClassTag

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.testkit.TestKit

object ProbeOps {
  implicit class ExpectMessageTypeAndAssert[T](val probe: TestProbe[T]) extends ExpectAndAssert[T] {
    protected def expectMessageType[S <: T: ClassTag](): S = probe.expectMessageType
  }

  implicit class ClassicExpectMessageTypeAndAssert(val tk: TestKit) extends ExpectAndAssert[Any] {
    override protected def expectMessageType[S: ClassTag](): S = tk.expectMsgType
  }
}

trait ExpectAndAssert[T] {
  protected def expectMessageType[S <: T: ClassTag](): S

  final def asserting[S <: T: ClassTag, A](assertion: S => A): A = {
    val received = expectMessageType[S]()
    assertion(received)
  }

  final def asserting[S <: T: ClassTag](assertion: S => Unit): Unit = asserting[S, Unit](assertion)
}
