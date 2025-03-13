package com.iterable.pulsarexample.part5

import com.iterable.pulsarexample.part1.PulsarPekkoSource
import com.iterable.pulsarexample.part1.ShutdownHook
import com.iterable.pulsarexample.part2.StreamParallelism
import com.iterable.pulsarexample.part3.LimitedMessageProcessor
import com.iterable.pulsarexample.part3.PermittedTaskExecutor
import com.iterable.pulsarexample.part4.Workload
import com.iterable.pulsarexample.util.PulsarClient
import com.iterable.pulsarexample.util.PulsarClientWrapper
import com.iterable.pulsarexample.util.PulsarConsumer
import org.apache.pekko.Done
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import org.reactivestreams.Publisher
import com.iterable.pulsarexample.util.ProcessingResult.ProcessFailure
import com.iterable.pulsarexample.util.ProcessingResult.ProcessSuccess
import PulsarClientWrapper.ConsumerConfig
import com.iterable.pulsarexample.util.MetricsCollector
import com.iterable.pulsarexample.util.ProcessedMessage
import com.iterable.pulsarexample.util.ProcessingResult
import zio.Runtime
import zio.Semaphore
import zio.Unsafe
import zio.ZIO
import zio.stream.ZStream

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

/*
 * Steps to create an ordered stream:
 *
 * 1. Pekko Stream -> Reactive Stream: Convert the Pekko Stream into a Reactive standard stream
 * 2. Reactive Stream -> ZIO Stream: Convert the Reactive Stream into a ZIO Stream
 * 3. ZIO Stream -> Permitted ZIO Stream: Pass each element in the stream through a  ZIO Semaphore that back pressures when
 *     we hit the workload (not global) concurrency limit).
 * 4. Permitted ZIO Stream -> grouped by key stream: Group each element in the ZIO stream into separate streams based on the Pulsar message key
 * 5. Grouped Streams -> to each stream apply the submit the processing function to our global task executor with a parallelism of 1
 * 5. Drain the results of the Grouped Streams since we don't need them.
 */

object OrderedStreamGenerator {

  def createStreamWithProcessingAndReturnShutdownHook[T](
    // Use the consumer for acking and nacking inside our Flow
    consumer: PulsarConsumer[T],
    source: PulsarPekkoSource[T],
    parallelism: StreamParallelism,
    processor: LimitedMessageProcessor[T],
    metrics: MetricsCollector,
    queueingTaskExecutor: PermittedTaskExecutor[() => Future[ProcessedMessage[T]], ProcessedMessage[T]],
    zioRuntime: Runtime[Any]
  )(implicit ec: ExecutionContext, materializer: Materializer): ShutdownHook = {

    /**
     * Broken out to make the main ZStream flow understandable
     */
    def taskExecutionFn(msg: Message[T]): Future[Future[ProcessedMessage[T]]] = {
      // The actual task we execute remains exactly the same.
      lazy val taskAcceptedFuture: Future[Future[ProcessedMessage[T]]] = queueingTaskExecutor.submit { () =>
        // Delegate to the permitting system's execution context
        Future.delegate {

          // We process and ack/nack for simplicity
          val processingResult: Future[ProcessedMessage[T]] =
            processor
              .processMessage(msg)
              .map(result => ProcessedMessage(result, msg))
              .recover { case e: Exception =>
                ProcessedMessage(ProcessFailure(e), msg)
              }
              .flatMap { processed =>
                val responseStartTime = System.nanoTime()

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
                        processed
                      })

                  case ProcessFailure(error) =>
                    metrics.recordError()

                    // Using the wrapped negativeAcknowledge method
                    Future {
                      consumer.negativeAcknowledge(processed.originalMessage)
                      val m =
                        s"Nacked message ${processed.originalMessage.getMessageId} with value ${processed.originalMessage.getValue.toString}"
                      metrics.recordNackTime(m, (System.nanoTime() - responseStartTime) / 10000)
                      processed
                    }
                }
              }

          processingResult
        }
      }

      taskAcceptedFuture
    }

    import zio.interop.reactivestreams.publisherToStream

    val pekkoSource = Source.fromGraph(source)

    // Convert Pekko Stream to a ZStream using Reactive streams
    val (shutdownHook: ShutdownHook, pub: Publisher[Message[T]]) =
      pekkoSource.toMat(Sink.asPublisher(fanout = true))(Keep.both).run()
    val zioSource: ZStream[Any, Throwable, Message[T]] = pub.toZIOStream()

    val permitsZio: zio.UIO[Semaphore] = Semaphore.make(parallelism.processingParallelism)

    val zioStreamFlow: ZIO[Any, Throwable, Unit] =
      // (3) Pass our ZStream through a stream level concurrency limiter to apply our workload parallelism limits
      permitsZio.flatMap { permits =>
        // Define the workload stream
        zioSource
          // (4) Create a separate stream within our stream for each key we are processing
          // Without step (4) this is unbounded and can blow up in our faces.
          .groupByKey(msg => msg.getKey, buffer = 1) { (_: String, msgStream: ZStream[Any, Throwable, Message[T]]) =>
            msgStream.mapZIOPar(1) { msg =>

              /*
               * Apply global concurrency limits by waiting on the outer Future[Future[ProcessedMessage[T]]] to complete
               * then waiting on the inner Future.
               */

              // (5) Submit the processing function to our global task executor with a parallelism of 1
              val taskAcceptedFuture = taskExecutionFn(msg)

              val zioTask: ZIO[Any, Throwable, ProcessedMessage[T]] =
                ZIO
                  .fromFuture(_ =>
                    {
                      taskAcceptedFuture
                    }.flatten
                  )

              permits.withPermit(zioTask)
            }
          }
          // (6) This is the equivalent of sink, run the stream and ignore the results
          .runDrain
      }

    Unsafe.unsafe { implicit unsafe =>
      zioRuntime.unsafe.runToFuture(zioStreamFlow)
    }

    shutdownHook
  }

  def createOrderedConsumerFlow[T](
    client: PulsarClient,
    schema: Schema[T],
    queueingTaskExecutor: PermittedTaskExecutor[() => Future[ProcessedMessage[T]], ProcessedMessage[T]],
    processor: LimitedMessageProcessor[T],
    metrics: MetricsCollector,
    zioRuntime: Runtime[Any]
  )(implicit
    executionContext: ExecutionContext,
    materializer: Materializer
  ): Flow[Workload, (String, ShutdownHook), NotUsed] = {
    Flow[Workload]
      // Create a consumer for the workload
      .mapAsync(1) { workload =>
        for {
          consumer <-
            PulsarClientWrapper
              .createConsumer(
                client,
                ConsumerConfig(
                  List(workload.topic),
                  "test-subscription",
                  subscriptionType = SubscriptionType.Key_Shared
                ),
                schema
              )
        } yield {
          (workload, consumer)
        }
      }
      // Follow the same steps as in part 3 to create a new stream for the workload
      .map { case (workload: Workload, consumer: PulsarConsumer[T]) =>
        val source = new PulsarPekkoSource(consumer)

        val shutdownHook: ShutdownHook = createStreamWithProcessingAndReturnShutdownHook(
          consumer = consumer,
          source = source,
          parallelism = workload.streamParallelism,
          processor = processor,
          metrics = metrics,
          queueingTaskExecutor = queueingTaskExecutor,
          zioRuntime = zioRuntime
        )

        (workload.workloadName, shutdownHook)
      }
  }

}
