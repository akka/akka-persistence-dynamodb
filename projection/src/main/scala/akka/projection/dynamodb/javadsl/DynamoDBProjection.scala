/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.javadsl

import java.util.Optional
import java.util.function.Supplier

import scala.jdk.OptionConverters._

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.scaladsl
import akka.projection.internal.HandlerAdapter
import akka.projection.internal.JavaToScalaBySliceSourceProviderAdapter
import akka.projection.javadsl.AtLeastOnceProjection
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider

@ApiMayChange
object DynamoDBProjection {

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * Compared to [[DynamoDBProjection.atLeastOnce]] the [[Handler]] is not storing the projected result in DynamoDB, but
   * is integrating with something else.
   *
   * It stores the offset in a DynamoDB table after the `handler` has processed the envelope. This means that if the
   * projection is restarted from previously stored offset then some elements may be processed more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first. This window
   * can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned `AtLeastOnceProjection`. The default
   * settings for the window is defined in configuration section `akka.projection.at-least-once`.
   */
  def atLeastOnceAsync[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[Envelope]],
      system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    scaladsl.DynamoDBProjection
      .atLeastOnceAsync[Offset, Envelope](
        projectionId,
        settings.toScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => HandlerAdapter(handler.get()))(system)
      .asInstanceOf[AtLeastOnceProjection[Offset, Envelope]]
  }

}
