package com.iterable.pulsarexample.part2

import com.iterable.pulsarexample.part1.PulsarPekkoSource
import com.iterable.pulsarexample.util.PulsarClient
import com.iterable.pulsarexample.util.PulsarClientSetup
import com.iterable.pulsarexample.util.PulsarClientWrapper
import com.iterable.pulsarexample.util.PulsarProducer
import com.iterable.pulsarexample.util.TestNamespace
import com.iterable.pulsarexample.util.TestPayload
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.SubscriptionType
import PulsarClientWrapper.ConsumerConfig
import PulsarClientWrapper.ProducerConfig
import com.iterable.pulsarexample.util.SingleStreamCollector

import scala.concurrent.duration.*
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

/**
 * Demonstrates how to set up and run a single Pulsar-Pekko stream with proper error handling and resource management.
 * This example shows direct stream composition and proper shutdown handling.
 */
object SingleMessageExample {

  val logger = Logger("SingleStreamExample")

  // Example topic name
  val testTenant = "testing"
  val testNamespace = "part1"
  val testTopic = s"persistent://${testTenant}/${testNamespace}/test-topic"

  def main(): Unit = {

    // Create actor system and materializer for Pekko Streams
    implicit val system: ActorSystem = ActorSystem("PulsarPekkoExample")
    implicit val materializer: Materializer = Materializer(system)

    implicit val ec: ExecutionContext = system.dispatcher

    /**
     * Handles graceful shutdown of resources
     */
    def performGracefulShutdown(
      producer: PulsarProducer[TestPayload],
      client: PulsarClient,
      admin: PulsarAdmin,
      namespace: TestNamespace
    ): Future[Unit] = {
      // Step 2: Close producer
      logger.info("Closing producer...")
      producer.closeAsync().transformWith { _ =>
        // Step 3: Clean up namespace and tenant
        logger.info("Cleaning up namespace and tenant...")
        PulsarClientSetup.cleanup(admin, namespace) match {
          case Success(_) =>
            // Step 4: Close client
            logger.info("Closing Pulsar client...")
            client.closeAync()
          case Failure(ex) =>
            logger.info(s"Failed to clean up namespace: ${ex.getMessage}")
            Future.failed(ex)
        }
      }
    }

    // Create producer configuration
    val producerConfig = ProducerConfig(
      topic = testTopic,
      name = Some("single-producer"),
      batchingEnabled = false
    )

    // Create consumer configuration
    val consumerConfig = ConsumerConfig(
      topics = List(testTopic),
      subscriptionName = "test-subscription",
      subscriptionType = SubscriptionType.Shared,
      receiverQueueSize = 100
    )

    // Configure stream parallelism
    val parallelism = StreamParallelism(
      processingParallelism = 3, // Process 3 messages concurrently
      acknowledgmentParallelism = 6 // Acknowledge 2 messages concurrently
    )

    // Create metrics collector
    val metrics = new SingleStreamCollector(logger)

    // Set up the test environment
    PulsarClientSetup.setup("testing", "part1") match {
      case Success((client, admin, namespace)) =>
        // Compose and run the stream directly
        for {
          consumer <- {
            logger.info("Trying to create consumer")
            PulsarClientWrapper
              .createConsumer(client, consumerConfig, TestPayload.schema)
          }
          producer <- {
            logger.info("Trying to create producer")
            PulsarClientWrapper
              .createProducer(client, producerConfig, TestPayload.schema)
          }
          _ <- {
            logger.info("Trying to send message")
            val payload = TestPayload(name = "single-test-message", 0)
            producer.send(payload).andThen(f => println(f))
          }
        } yield {

          val source = new PulsarPekkoSource(consumer)
          val processor = new SimpleMessageProcessor(producer, "single-consumer", 2000, 1000, logger, system, ec)

          // Create the source with parallel processing
          val (killSwitch, streamCompletionFuture) = PekkoStreamGenerator
            .createStreamFromSourceAndProcessingSteps[TestPayload](
              consumer = consumer,
              source = source,
              parallelism = parallelism,
              processor = processor,
              metrics = metrics
            )
            .viaMat(KillSwitches.single)(Keep.right)
            .toMat(Sink.ignore)(Keep.both)
            .run()

          // Handle stream completion and shutdown
          streamCompletionFuture.onComplete {
            case Success(_) =>
              logger.info("Stream completed successfully")
              performGracefulShutdown(producer, client, admin, namespace)

            case Failure(exception) =>
              logger.error(s"Stream failed: ${exception.getMessage}")
              performGracefulShutdown(producer, client, admin, namespace)
          }

          // Keep the application running
          Thread.sleep(30 * 1000)

          source.drainAndShutdown(Future.successful(()))
        }
      case Failure(exception) =>
        logger.info(s"Failed to set up test environment: ${exception.getMessage}")
        system.terminate()
    }

  }
}
