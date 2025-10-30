/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.s3fallback

import akka.Done
import com.typesafe.config.Config
import io.minio.BucketExistsArgs
import io.minio.ListObjectsArgs
import io.minio.MakeBucketArgs
import io.minio.MinioAsyncClient
import io.minio.RemoveObjectArgs
import io.minio.RemoveBucketArgs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.control.NonFatal

object MinioInteraction {
  val FutureDone = Future.successful(Done)

  def createBuckets(
      config: Config)(implicit minioClient: MinioAsyncClient, executionContext: ExecutionContext): Future[Done] = {
    val eventsBucket = config.getString("akka.persistence.s3-fallback-store.events-bucket")
    val snapshotsBucket = config.getString("akka.persistence.s3-fallback-store.snapshots-bucket")

    Seq(eventsBucket, snapshotsBucket).foldLeft(FutureDone) { (fut, bucket) =>
      fut.flatMap { _ =>
        val args = BucketExistsArgs.builder().bucket(bucket).build()
        minioClient
          .bucketExists(args)
          .asScala
          .flatMap { exists =>
            if (exists) FutureDone
            else {
              val args = MakeBucketArgs.builder().bucket(bucket).build()
              minioClient.makeBucket(args).asScala.map(_ => Done)(ExecutionContext.parasitic)
            }
          }
      }
    }
  }

  def deleteBuckets(
      config: Config)(implicit minioClient: MinioAsyncClient, executionContext: ExecutionContext): Future[Done] = {
    val eventsBucket = config.getString("akka.persistence.s3-fallback-store.events-bucket")
    val snapshotsBucket = config.getString("akka.persistence.s3-fallback-store.snapshots-bucket")

    Seq(eventsBucket, snapshotsBucket).foldLeft(FutureDone) { (fut, bucket) =>
      fut
        .flatMap { _ =>
          minioClient
            .listObjects(ListObjectsArgs.builder.bucket(bucket).build())
            .iterator
            .asScala
            .foldLeft(FutureDone) { (fut, result) =>
              fut.flatMap { _ =>
                try {
                  val item = result.get
                  minioClient
                    .removeObject(RemoveObjectArgs.builder.bucket(bucket).`object`(item.objectName).build())
                    .asScala
                    .map { _ =>
                      Done
                    }(ExecutionContext.parasitic)
                } catch {
                  case NonFatal(ex) => Future.failed(ex)
                }
              }(ExecutionContext.parasitic)
            }
            .flatMap { _ =>
              minioClient
                .removeBucket(RemoveBucketArgs.builder.bucket(bucket).build())
                .asScala
                .map(_ => Done)(ExecutionContext.parasitic)
            }
        }
    }
  }
}
