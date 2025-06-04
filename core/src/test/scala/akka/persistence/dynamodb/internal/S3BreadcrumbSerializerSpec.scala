/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class S3BreadcrumbSerializerSpec extends AnyWordSpec with TestSuite with Matchers {
  "S3BreadcrumbSerializer" should {
    val system = ActorSystem(Behaviors.empty, "test")
    val serialization = SerializationExtension(system)

    "round-trip" in {
      val bucket = Iterator
        .continually(Random.nextPrintableChar())
        .take(6)
        .foldLeft(new StringBuilder(6)) { (sb, c) => sb.append(c) }
        .toString()
      val breadcrumb = S3Breadcrumb(bucket)
      val serializer = serialization.findSerializerFor(breadcrumb)
      val serId = serializer.identifier
      val manifest = Serializers.manifestFor(serializer, breadcrumb)
      val bytes = serialization.serialize(breadcrumb).get

      serialization.deserialize(bytes, serId, manifest).get shouldBe breadcrumb
    }
  }
}
