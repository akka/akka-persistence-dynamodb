/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.dynamodb.internal

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.annotation.InternalApi
import akka.persistence.Persistence

import scala.annotation.tailrec
import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.IteratorHasAsScala

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.concurrent.ConcurrentHashMap
import java.time.temporal.ChronoUnit

object MonotonicTimestamps extends ExtensionId[MonotonicTimestamps] {
  override def createExtension(system: ActorSystem[_]): MonotonicTimestamps = new MonotonicTimestamps(system)

  def get(system: ActorSystem[_]): MonotonicTimestamps = createExtension(system)

  @InternalApi
  private[internal] final class PerPlugin(
      system: ActorSystem[_],
      name: String,
      numRanges: Int,
      rangeForPid: String => Int) {
    // only written to by a single actor
    private val byPid = (0 until numRanges).map { _ => new ConcurrentHashMap[String, Record]() }.toVector
    private val rangeActors = (0 until numRanges).map { range =>
      val actorName =
        URLEncoder.encode(s"dynamodb-persistence-monotonic-timestamps-${name}-$range", StandardCharsets.UTF_8)
      val behavior =
        Behaviors
          .supervise(
            Behaviors
              .setup[Any] { context =>
                if (byPid(range).isEmpty) {
                  Behaviors.withTimers { timers =>
                    cleanerBehavior(range, SortedSet.empty, timers)
                  }
                } else {
                  val recordSet =
                    byPid(range).values.iterator.asScala
                      .foldLeft(SortedSet.empty[Record]) { (acc, v) =>
                        acc.incl(v)
                      }

                  Behaviors.withTimers { timers =>
                    scheduleNextCleanup(Instant.now(), recordSet.head.nextTimestamp, timers)
                    cleanerBehavior(range, recordSet, timers)
                  }
                }
              }
              .narrow[(Record, ActorRef[Done])])
          .onFailure(SupervisorStrategy.restart)

      system.systemActorOf(behavior, actorName)
    }.toVector

    def minTimestampFor(pid: String): Option[Instant] =
      byPid(rangeForPid(pid)).get(pid) match {
        case null   => None
        case record => Some(record.nextTimestamp)
      }

    def recordTimestampFor(pid: String, timestamp: Instant): Future[Done] = {
      rangeActors(rangeForPid(pid))
        .ask[Done]((Record(pid, timestamp.plus(1, ChronoUnit.MICROS)), _))(1.second, system.scheduler)
    }

    private def scheduleNextCleanup(now: Instant, nextTimestamp: Instant, timers: TimerScheduler[Any]): Unit = {
      val nextCleanupIn = {
        val millis =
          try {
            JDuration.between(now, nextTimestamp).toMillis / 2
          } catch {
            case _: ArithmeticException => 10000 // ten second maximum
          }

        // minimum 1 second, maximum 10 seconds
        (millis.min(10000).max(1)).millis
      }

      timers.startSingleTimer(Cleanup, Cleanup, nextCleanupIn)
    }

    private[internal] def cleanerBehavior(
        range: Int,
        recordSet: SortedSet[Record],
        timers: TimerScheduler[Any]): Behavior[Any] =
      Behaviors.receive { (context, msg) =>
        msg match {
          case Cleanup =>
            // next timestamp will be greater than this
            val keepAfter = InstantFactory.now()

            // adding the nano ensures that this will compare greater than
            // any record with timestamp of keepAfter
            // "" as pid (not legal pid) will compare less than any record with same timestamp
            // net effect is to swap the clusivity of rangeFrom/rangeTo
            val pivotRecord = Record("", keepAfter.plusNanos(1))

            val recordsToKeep = recordSet.rangeFrom(pivotRecord)
            val recordsToDrop = recordSet.rangeTo(pivotRecord)

            val kept =
              recordsToDrop.foldLeft(recordsToKeep) { (rtk, record) =>
                val pid = record.pid

                if (byPid(range).remove(pid, record)) rtk
                else {
                  context.log.warn(
                    "Concurrent modification of state: this should not happen.  Report issue at github.com/akka/akka-persistence-dynamodb")
                  rtk.incl(byPid(range).get(pid))
                }
              }

            if (kept.nonEmpty) { scheduleNextCleanup(keepAfter, kept.head.nextTimestamp, timers) }

            cleanerBehavior(range, kept, timers)

          case (rec: Record, replyTo: ActorRef[Nothing]) =>
            val pid = rec.pid
            val nextRecordSet =
              byPid(range).get(pid) match {
                case null =>
                  if (byPid(range).putIfAbsent(pid, rec) eq null) {
                    replyTo.unsafeUpcast[Done] ! Done
                    recordSet.incl(rec)
                  } else {
                    context.log.warn(
                      "Timestamp not updated for persistence ID [{}]. " +
                      "Report issue at github.com/akka/akka-persistence-dynamodb",
                      pid)

                    // no reply
                    recordSet
                  }

                case oldRecord =>
                  if (oldRecord.nextTimestamp.isBefore(rec.nextTimestamp)) {
                    oldRecord match {
                      case expected if expected eq oldRecord =>
                        replyTo.unsafeUpcast[Done] ! Done
                        recordSet.excl(oldRecord).incl(rec)

                      case unexpected =>
                        context.log.warn(
                          "Timestamp not updated for persistence ID [{}]. " +
                          "Report issue at github.com/akka/akka-persistence-dynamodb",
                          pid)
                        recordSet.excl(oldRecord).incl(unexpected)
                    }
                  } else {
                    context.log.warn(
                      "Ignoring attempt to set timestamp for persistence ID [{}] to earlier. " +
                      "existing=[{}] attempted=[{}]",
                      pid,
                      oldRecord.nextTimestamp,
                      rec.nextTimestamp)

                    replyTo.unsafeUpcast[Done] ! Done
                    recordSet
                  }
              }

            if (!timers.isTimerActive(Cleanup) && nextRecordSet.nonEmpty) {
              scheduleNextCleanup(Instant.now(), rec.nextTimestamp, timers)
            }

            cleanerBehavior(range, nextRecordSet, timers)

          case _ => Behaviors.unhandled
        }
      }
  }

  private val Cleanup = "Cleanup"

  private case class Record(pid: String, nextTimestamp: Instant)
  private object Record {
    implicit val ordering: Ordering[Record] =
      new Ordering[Record] {
        override def compare(x: Record, y: Record): Int =
          x.nextTimestamp.compareTo(y.nextTimestamp) match {
            case 0      => x.pid.compareTo(y.pid)
            case result => result
          }
      }
  }
}

final class MonotonicTimestamps(system: ActorSystem[_]) extends Extension {
  import MonotonicTimestamps.PerPlugin

  private val persistenceExt = Persistence(system)
  private val numRanges =
    // minimize contention by having a number of ranges that's at least available processors
    Runtime.getRuntime.availableProcessors match {
      case lt2 if lt2 < 2          => 1
      case gt1024 if gt1024 > 1024 => 1024
      case numProcs =>
        val clz = Integer.numberOfLeadingZeros(numProcs - 1)
        1 << (32 - clz) // next highest power of 2
    }

  private val rawRanges = persistenceExt.sliceRanges(numRanges)
  private val starts = rawRanges.map(_.head).toArray
  private val rangeForPid = (pid: String) => {
    val slice = persistenceExt.sliceForPersistenceId(pid)

    @tailrec
    def iter(lo: Int, hi: Int): Int =
      if ((lo + 1) >= hi) lo
      else {
        val pivot = (lo + hi) / 2
        val p = starts(pivot)

        if (p == slice) pivot
        else if (p < slice) iter(pivot, hi)
        else iter(lo, pivot)
      }

    iter(0, starts.length)
  }

  private val perPlugin = new ConcurrentHashMap[String, PerPlugin]()

  def minTimestampFor(plugin: String): String => Option[Instant] = {
    val pp =
      perPlugin.computeIfAbsent(plugin, _ => new PerPlugin(system, plugin, numRanges, rangeForPid))

    pp.minTimestampFor _
  }

  def recordTimestampFor(plugin: String): (String, Instant) => Future[Done] = {
    val pp =
      perPlugin.computeIfAbsent(plugin, _ => new PerPlugin(system, plugin, numRanges, rangeForPid))

    pp.recordTimestampFor _
  }
}
