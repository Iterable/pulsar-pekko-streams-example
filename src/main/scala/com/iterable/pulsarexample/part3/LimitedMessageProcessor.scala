package com.iterable.pulsarexample.part3

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
import scala.concurrent.duration.DurationLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Random

trait LimitedMessageProcessor[T] {
  def processMessage(message: Message[T])(implicit executionContext: ExecutionContext): Future[ProcessingResult]
}

/**
 * To simplify things have one message processor for all streams.
 *
 * Its got basic capabilities to log and simulate processing time and errors.
 */
class MapMessageProcessor(
  producers: Map[String, PulsarProducer[TestPayload]],
  name: String,
  sleep: Long,
  checkInterval: Long,
  logger: Logger,
  system: ActorSystem,
  executionContext: ExecutionContext,
  logIndividualMessages: Boolean = true,
  allowFailures: Boolean = true
) extends LimitedMessageProcessor[TestPayload] {

  val inProgress = AtomicInteger(0)

  val cancellable: Cancellable = system.scheduler.scheduleAtFixedRate(
    initialDelay = 0.seconds,
    interval = checkInterval.millis
  )(() => {
    logger.info(s"[${name}] ${inProgress.get()} messages in progress")
  })(executionContext)

  system.registerOnTermination {
    cancellable.cancel()
    logger.info("Cancelling in progress monitor")
  }

  // Make sure we're using the globally limited context
  def processMessage(
    message: Message[TestPayload]
  )(implicit executionContext: ExecutionContext): Future[ProcessingResult] = {
    val inProgressCount = inProgress.addAndGet(1)

    val payload = message.getValue
    if (logIndividualMessages) {
      logger.info(
        s"${name}: ${payload.name} publish #${payload.numPublishes}"
      )
    }

    Future {
      Thread.sleep(sleep)

      if (allowFailures && Random.nextInt(10) == 0) {
        logger.error(s"Simulating failure for message: ${payload.name} publish #${payload.numPublishes}")
        inProgress.decrementAndGet()
        Future.successful(ProcessFailure(new Exception("Simulating processing failure")))
      } else {
        val topicName = message.getTopicName.stripSuffix("-partition-0")
        // Sleep for X milliseconds to simulate processing time
        val producer = producers(topicName)
        producer.send(TestPayload(payload)).map { _ =>
          inProgress.decrementAndGet()
          ProcessSuccess
        }
      }
    }.flatten
  }
}
