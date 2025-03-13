package com.iterable.pulsarexample.part2

import com.iterable.pulsarexample.part1.PulsarPekkoSource
import com.iterable.pulsarexample.util.ProcessingResult
import com.iterable.pulsarexample.util.PulsarConsumer
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pulsar.client.api.Message
import ProcessingResult.ProcessFailure
import ProcessingResult.ProcessSuccess
import com.iterable.pulsarexample.util.MetricsCollector
import com.iterable.pulsarexample.util.ProcessedMessage

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

case class StreamParallelism(
  processingParallelism: Int,
  acknowledgmentParallelism: Int
)

/**
 * Add two functions onto the pulsar consumer:
 *
 *   1. A step to process a message by executing a function
 */
object PekkoStreamGenerator {

  def createStreamFromSourceAndProcessingSteps[T](
    // Use the consumer for acking and nacking inside our Flow
    consumer: PulsarConsumer[T],
    // The source is the already created
    source: PulsarPekkoSource[T],
    parallelism: StreamParallelism,
    processor: SimpleProcessor[T],
    metrics: MetricsCollector
  )(implicit ec: ExecutionContext): Source[ProcessingResult, _] = {

    val processingFlow: Flow[Message[T], ProcessingResult, _] = Flow[Message[T]]
      .mapAsyncUnordered(parallelism.processingParallelism) { message =>
        val startTime = System.nanoTime()

        processor
          .processMessage(message)
          .map { result =>
            val m = s"Processed message ${message.getMessageId} with value ${message.getValue.toString}"
            val processingTime = System.nanoTime() - startTime
            metrics.recordProcessingTime(m, processingTime / 1000)

            ProcessedMessage(result, message)
          }
          .recover { case e: Exception =>
            ProcessedMessage(ProcessFailure(e), message)
          }
      }
      .mapAsyncUnordered(parallelism.acknowledgmentParallelism) { processed =>
        val responseStartTime = System.nanoTime()

        // Using the wrapped acknowledgment methods that return Scala Futures
        processed.result match {
          case ProcessSuccess =>
            consumer
              .acknowledgeAsync(processed.originalMessage)
              .recoverWith { case e: Exception =>
                System.err.println(e)
                metrics.recordError()
                consumer.acknowledgeAsync(processed.originalMessage)
              }
              .map(_ => {
                val m =
                  s"Acked message ${processed.originalMessage.getMessageId} with value ${processed.originalMessage.getValue.toString}"
                metrics.recordAckTime(m, (System.nanoTime() - responseStartTime) / 10000)
                processed.result
              })

          case ProcessFailure(error) =>
            metrics.recordError()

            // Using the wrapped negativeAcknowledge method
            Future {
              consumer.negativeAcknowledge(processed.originalMessage)
              val m =
                s"Nacked message ${processed.originalMessage.getMessageId} with value ${processed.originalMessage.getValue.toString}"
              metrics.recordNackTime(m, (System.nanoTime() - responseStartTime) / 10000)
              processed.result
            }
        }
      }

    Source
      .fromGraph(source)
      // Let the Pulsar consumer or Pulsar server buffer rather than Pekko.
      .withAttributes(Attributes.inputBuffer(0, 1))
      .via(processingFlow)
  }

  /**
   * Alternatively instead of passing in a specific function allow passing in a Flow that does any number of steps with
   * their own restrictions before acknowledging.
   */
  def createStreamFromSourceAndProcssesingFlow[T](
    // Use the consumer for acking and nacking inside our Flow
    consumer: PulsarConsumer[T],
    // The source is the already created
    source: PulsarPekkoSource[T],
    processingFlow: Flow[Message[T], ProcessedMessage[T], _],
    parallelism: StreamParallelism,
    metrics: Option[MetricsCollector] = None
  )(implicit ec: ExecutionContext): Source[ProcessingResult, _] = {

    val flow: Flow[Message[T], ProcessingResult, _] = processingFlow
      .mapAsyncUnordered(parallelism.acknowledgmentParallelism) { processed =>
        val responseStartTime = System.nanoTime()

        // Using the wrapped acknowledgment methods that return Scala Futures
        val acknowledgmentFuture = processed.result match {
          case ProcessSuccess =>
            metrics.foreach(_.recordSuccess())

            consumer
              .acknowledgeAsync(processed.originalMessage)
              .recoverWith { case e: Exception =>
                System.err.println(e)
                metrics.foreach(_.recordError())
                consumer.acknowledgeAsync(processed.originalMessage)
              }
              .map(_ => {
                val m =
                  s"Acked message ${processed.originalMessage.getMessageId} with value ${processed.originalMessage.getValue.toString}"
                metrics.foreach(_.recordAckTime(m, (System.nanoTime() - responseStartTime) / 10000))
                processed.result
              })

          case ProcessFailure(error) =>
            metrics.foreach(_.recordError())

            // Using the wrapped negativeAcknowledge method
            Future {
              consumer.negativeAcknowledge(processed.originalMessage)
              val m =
                s"Nacked message ${processed.originalMessage.getMessageId} with value ${processed.originalMessage.getValue.toString}"
              metrics.foreach(_.recordNackTime(m, System.nanoTime() - responseStartTime))
              processed.result
            }
        }

        acknowledgmentFuture
      }

    Source
      .fromGraph(source)
      .via(flow)
  }
}
