/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb

import akka.persistence.dynamodb.internal.MinioInteraction
import io.minio.MinioAsyncClient
import org.scalatest.Suite

trait TestDbAndS3Lifecycle extends TestDbLifecycle { this: Suite =>
  lazy val localS3 = settings.s3FallbackSettings.minioLocal.enabled

  lazy val _minioClient: MinioAsyncClient =
    MinioAsyncClient
      .builder()
      .endpoint("http://localhost:9000")
      .credentials("akka", "2b7b1446")
      .build()

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (localS3) {
      MinioInteraction.createBuckets(typedSystem.settings.config)(_minioClient)
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    if (localS3) {
      MinioInteraction.deleteBuckets(typedSystem.settings.config)(_minioClient)
    }
  }
}
