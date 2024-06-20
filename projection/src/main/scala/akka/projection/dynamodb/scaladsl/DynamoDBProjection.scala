/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.dynamodb.util.ClientProvider
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.internal.DynamoDBProjectionImpl
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.OffsetStoredByHandler
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.scaladsl.AtLeastOnceProjection
import akka.projection.scaladsl.ExactlyOnceProjection
import akka.projection.scaladsl.GroupedProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider

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
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => Handler[Envelope])(implicit system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForAtLeastOnce(sourceProvider, handler, offsetStore)(
        system.executionContext,
        system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = SingleHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * The offset is stored in DynamoDB in the same transaction as the `TransactWriteItem`s returned by the `handler`.
   */
  def exactlyOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => DynamoDBTransactHandler[Envelope])(implicit
      system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForExactlyOnce(sourceProvider, handler, offsetStore)(
        system.executionContext,
        system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = ExactlyOnce(),
      handlerStrategy = SingleHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
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
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => DynamoDBTransactHandler[Seq[Envelope]])(implicit
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForExactlyOnceGrouped(sourceProvider, handler, offsetStore)(
        system.executionContext,
        system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = ExactlyOnce(),
      handlerStrategy = GroupedHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
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
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => Handler[Seq[Envelope]])(implicit system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForAtLeastOnceGrouped(sourceProvider, handler, offsetStore)(
        system.executionContext,
        system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = OffsetStoredByHandler(),
      handlerStrategy = GroupedHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  private def timestampOffsetBySlicesSourceProvider(
      sourceProvider: SourceProvider[_, _]): Option[BySlicesSourceProvider] = {
    sourceProvider match {
      case provider: BySlicesSourceProvider => Some(provider)
      case _                                => None // source provider is not using slices
    }
  }

}
