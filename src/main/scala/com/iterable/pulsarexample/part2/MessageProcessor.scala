package com.iterable.pulsarexample.part2

import com.iterable.pulsarexample.util.ProcessingResult
import com.iterable.pulsarexample.util.PulsarProducer
import com.iterable.pulsarexample.util.TestPayload
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Cancellable
import org.apache.pulsar.client.api.Message
import ProcessingResult.ProcessFailure
import ProcessingResult.ProcessSuccess

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationLong
import scala.util.Random

trait SimpleProcessor[T] {
  def processMessage(message: Message[T]): Future[ProcessingResult]
}

class SimpleMessageProcessor(
  producer: PulsarProducer[TestPayload],
  name: String,
  sleep: Long,
  checkInterval: Long,
  logger: Logger,
  system: ActorSystem,
  implicit val executionContext: ExecutionContext
) extends SimpleProcessor[TestPayload] {

  val inProgress = AtomicInteger(0)

  val cancellable: Cancellable = system.scheduler.scheduleAtFixedRate(
    initialDelay = 0.seconds,
    interval = checkInterval.millis
  )(() => {
    logger.info(s"[${name}] ${inProgress.get()} messages in progress")
  })

  system.registerOnTermination {
    cancellable.cancel()
    logger.info("Cancelling in progress monitor")
  }

  def processMessage(message: Message[TestPayload]): Future[ProcessingResult] = {
    val inProgressCount = inProgress.addAndGet(1)

    val payload = message.getValue
    logger.info(
      s"${name}: ${payload.name} publish #${payload.numPublishes}"
    )

    Future {
      Thread.sleep(sleep)

      if (Random.nextInt(10) == 0) {
        logger.error(s"Simulating failure for message: ${payload.name} publish #${payload.numPublishes}")
        inProgress.decrementAndGet()
        Future.successful(ProcessFailure(new Exception("Simulating processing failure")))
      } else {
        // Sleep for X milliseconds to simulate processing time

        producer.send(TestPayload(payload)).map { _ =>
          inProgress.decrementAndGet()
          ProcessSuccess
        }
      }
    }.flatten
  }
}
