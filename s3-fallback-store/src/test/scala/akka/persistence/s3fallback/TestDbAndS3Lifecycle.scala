/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.s3fallback

import akka.persistence.dynamodb.TestDbLifecycle
import io.minio.MinioAsyncClient
import org.scalatest.Suite

trait TestDbAndS3Lifecycle extends TestDbLifecycle { this: Suite =>
  lazy val s3FallbackSettings = S3FallbackSettings(
    typedSystem.settings.config.getConfig("akka.persistence.s3-fallback-store"))
  lazy val localS3 = s3FallbackSettings.minioLocal.enabled

  lazy val minioClient: MinioAsyncClient =
    MinioAsyncClient
      .builder()
      .endpoint("http://localhost:9000")
      .credentials("akka", "2b7b1446")
      .build()

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (localS3) {
      MinioInteraction.createBuckets(typedSystem.settings.config)(minioClient, typedSystem.executionContext)
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    if (localS3) {
      MinioInteraction.deleteBuckets(typedSystem.settings.config)(minioClient, typedSystem.executionContext)
    }
  }
}
