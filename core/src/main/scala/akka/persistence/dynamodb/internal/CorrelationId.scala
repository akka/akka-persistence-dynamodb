/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.dynamodb.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[dynamodb] object CorrelationId {
  def toLogText(correlationid: Option[String]) = correlationid match {
    case Some(id) => s", correlation [$id]"
    case None     => ""
  }
}
