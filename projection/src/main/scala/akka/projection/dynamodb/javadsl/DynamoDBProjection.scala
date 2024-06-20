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
import akka.projection.dynamodb.internal.DynamoDBTransactGroupedHandlerAdapter
import akka.projection.dynamodb.internal.DynamoDBTransactHandlerAdapter
import akka.projection.dynamodb.scaladsl
import akka.projection.internal.GroupedHandlerAdapter
import akka.projection.internal.HandlerAdapter
import akka.projection.internal.JavaToScalaBySliceSourceProviderAdapter
import akka.projection.javadsl.AtLeastOnceProjection
import akka.projection.javadsl.ExactlyOnceProjection
import akka.projection.javadsl.GroupedProjection
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider

@ApiMayChange
object DynamoDBProjection {

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a DynamoDB table after the `handler` has processed the envelope. This means that if the
   * projection is restarted from previously stored offset then some elements may be processed more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first. This window
   * can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned `AtLeastOnceProjection`. The default
   * settings for the window is defined in configuration section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[Envelope]],
      system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    scaladsl.DynamoDBProjection
      .atLeastOnce[Offset, Envelope](
        projectionId,
        settings.toScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => HandlerAdapter(handler.get()))(system)
      .asInstanceOf[AtLeastOnceProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * The offset is stored in DynamoDB in the same transaction as the `TransactWriteItem`s returned by the `handler`.
   */
  def exactlyOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[DynamoDBTransactHandler[Envelope]],
      system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    scaladsl.DynamoDBProjection
      .exactlyOnce[Offset, Envelope](
        projectionId,
        settings.toScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new DynamoDBTransactHandlerAdapter(handler.get()))(system)
      .asInstanceOf[ExactlyOnceProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
   * window can be defined with [[GroupedProjection.withGroup]] of the returned `GroupedProjection`. The default
   * settings for the window is defined in configuration section `akka.projection.grouped`.
   *
   * The offset is stored in DynamoDB in the same transaction as the `TransactWriteItem`s returned by the `handler`.
   */
  def exactlyOnceGroupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[DynamoDBTransactHandler[java.util.List[Envelope]]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {
    scaladsl.DynamoDBProjection
      .exactlyOnceGroupedWithin[Offset, Envelope](
        projectionId,
        settings.asScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new DynamoDBTransactGroupedHandlerAdapter(handler.get()))(system)
      .asInstanceOf[GroupedProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
   * window can be defined with [[GroupedProjection.withGroup]] of the returned `GroupedProjection`. The default
   * settings for the window is defined in configuration section `akka.projection.grouped`.
   *
   * The offset is stored in DynamoDB immediately after the `handler` has processed the envelopes, but that is still
   * with at-least-once processing semantics. This means that if the projection is restarted from previously stored
   * offset the previous group of envelopes may be processed more than once.
   */
  def atLeastOnceGroupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[java.util.List[Envelope]]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {
    scaladsl.DynamoDBProjection
      .atLeastOnceGroupedWithin[Offset, Envelope](
        projectionId,
        settings.asScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new GroupedHandlerAdapter(handler.get()))(system)
      .asInstanceOf[GroupedProjection[Offset, Envelope]]
  }

}
