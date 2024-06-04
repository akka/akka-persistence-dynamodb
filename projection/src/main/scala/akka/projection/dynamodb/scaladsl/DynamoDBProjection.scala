/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.scaladsl

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.dynamodb.ClientProvider
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.internal.DynamoDBProjectionImpl
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.scaladsl.AtLeastOnceProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.FlowWithContext

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
      DynamoDBProjectionImpl.adaptedHandlerForAtLeastOnceAsync(sourceProvider, handler, offsetStore)(
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

  private def timestampOffsetBySlicesSourceProvider(
      sourceProvider: SourceProvider[_, _]): Option[BySlicesSourceProvider] = {
    sourceProvider match {
      case provider: BySlicesSourceProvider => Some(provider)
      case _                                => None // source provider is not using slices
    }
  }

}
