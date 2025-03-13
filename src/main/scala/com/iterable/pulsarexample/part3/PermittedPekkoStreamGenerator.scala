package com.iterable.pulsarexample.part3

import com.iterable.pulsarexample.part1.PulsarPekkoSource
import com.iterable.pulsarexample.part1.ShutdownHook
import com.iterable.pulsarexample.part2.StreamParallelism
import com.iterable.pulsarexample.util.ProcessingResult
import com.iterable.pulsarexample.util.PulsarConsumer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pulsar.client.api.Message
import ProcessingResult.ProcessFailure
import ProcessingResult.ProcessSuccess
import com.iterable.pulsarexample.util.MetricsCollector
import com.iterable.pulsarexample.util.ProcessedMessage

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

object PermittedPekkoStreamGenerator {

  def createStreamWithProcessingAndReturnShutdownHook[T](
    // Use the consumer for acking and nacking inside our Flow
    consumer: PulsarConsumer[T],
    source: PulsarPekkoSource[T],
    parallelism: StreamParallelism,

    // A processor using the execution context of its caller (the PermittedTaskExecutor)
    processor: LimitedMessageProcessor[T],
    metrics: MetricsCollector,

    // The global concurrency limited executor
    queueingTaskExecutor: PermittedTaskExecutor[() => Future[ProcessedMessage[T]], ProcessedMessage[T]]
  )(implicit ec: ExecutionContext, materializer: Materializer): ShutdownHook = {

    val processingFlow: Flow[Message[T], ProcessedMessage[T], NotUsed] = Flow[Message[T]]

      /*
       * Apply a global concurrency limit by passing a task to the permitted executor. The permitted handles
       * applying the limit and we just have to use the callbacks it provides to control the stream. The permitted
       * executor will backpressure if the global limit is reached and prevent further tasks from being submitted.
       *
       * We return a Future[Future[ProcessedMessage[T]]] because we don't want to wait for the message to be processed.
       *
       * The outer Future is a callback that finishes as soon as the permitted executor accepts the task.
       *
       * The inner Future is a callback that completes when processing the message finishes.
       *
       * We need the outer Future because we don't want to wait for a task to complete before submitting the next task.
       *
       * Pekko Streams will wait for the outer Future (the task submission) to complete and then in the next step
       * we can apply our stream level concurrency limits to the inner Future (the processing of the message).
       */
      .mapAsync(1) { message =>

        val taskAcceptedFuture: Future[Future[ProcessedMessage[T]]] = queueingTaskExecutor.submit { () =>
          // Delegate to the permitting system's execution context
          Future.delegate {
            processor
              .processMessage(message)
              .map { result =>
                ProcessedMessage(result, message)
              }
              .recover { case e: Exception =>
                ProcessedMessage(ProcessFailure(e), message)
              }
          }
        }

        taskAcceptedFuture
      }
      // Apply the stream level concurrency limits and backpressure if the workload has reached the desired concurrency
      .mapAsyncUnordered(parallelism.processingParallelism) { (ft: Future[ProcessedMessage[T]]) =>
        ft
      }

    // Separately from the global system acknowledge messages back to Pulsar, this may be desirable
    // when you have a final step that is much faster than the intermediate steps.
    val ackFlow = Flow[ProcessedMessage[T]]
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
      .withAttributes(Attributes.inputBuffer(0, 1))
      .via(processingFlow)
      .via(ackFlow)
      .to(Sink.ignore)
      .run()
  }

  /**
   * This Flow is used to generate each workload stream, it returns a handle to shut down the stream.
   *
   * The Flow allows each stream to have its own concurrency limits and processor.
   */
  def buildMultiConsumerFlow[T](
    metrics: MetricsCollector,
    queueingTaskExecutor: PermittedTaskExecutor[() => Future[ProcessedMessage[T]], ProcessedMessage[T]]
  )(implicit
    ec: ExecutionContext,
    materializer: Materializer
  ): Flow[(PulsarConsumer[T], StreamParallelism, LimitedMessageProcessor[T]), ShutdownHook, NotUsed] = {

    Flow[(PulsarConsumer[T], StreamParallelism, LimitedMessageProcessor[T])].map { (consumer, parallelism, processor) =>
      val source = new PulsarPekkoSource(consumer)

      val shutdownHook: ShutdownHook = createStreamWithProcessingAndReturnShutdownHook(
        consumer = consumer,
        source = source,
        parallelism = parallelism,
        processor = processor,
        metrics = metrics,
        queueingTaskExecutor = queueingTaskExecutor
      )

      shutdownHook
    }
  }

}
