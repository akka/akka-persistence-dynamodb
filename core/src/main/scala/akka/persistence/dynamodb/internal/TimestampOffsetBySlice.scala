/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import akka.annotation.InternalApi
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset

// FIXME maybe move to akka-persistence-query if it should be shared with r2dbc

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TimestampOffsetBySlice {
  val empty: TimestampOffsetBySlice = new TimestampOffsetBySlice(Map.empty)

  def apply(offsets: Map[Int, TimestampOffset]): TimestampOffsetBySlice =
    new TimestampOffsetBySlice(offsets)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TimestampOffsetBySlice(val offsets: Map[Int, TimestampOffset]) extends Offset {
  override def toString = s"TimestampOffsetBySlice($offsets)"
}
