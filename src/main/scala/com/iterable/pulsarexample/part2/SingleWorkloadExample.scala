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
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.SubscriptionType
import PulsarClientWrapper.ConsumerConfig
import PulsarClientWrapper.ProducerConfig
import com.iterable.pulsarexample.util.SingleStreamCollector

import java.util.concurrent.Executors
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

/**
 * Example:
 *
 *   1. Create a single consumer and producer 2. Publish more test messages than we can process at one time. 3. Create a
 *      stream that processes those messages and republishes them in a loop. The stream will only process 2 of 3
 *      messages at once because we limit it. 4. Wait 30 seconds and then shut down the stream.
 */
object SingleWorkloadExample {

  val logger = Logger("SingleWorkloadExample")

  // Example topic name
  val testTenant = "testing"
  val testNamespace = "part2"
  val testTopic = s"persistent://${testTenant}/${testNamespace}/test-topic-multi"

  def main(args: Array[String]): Unit = {

    // Create actor system and materializer for Pekko Streams
    implicit val system: ActorSystem = ActorSystem("PulsarPekkoExample")
    implicit val materializer: Materializer = Materializer(system)

    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))

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

    // Create metrics collector
    val metrics = new SingleStreamCollector(logger)

    val brokerUrl = args.headOption.map(domain => s"pulsar://${domain}:6650").getOrElse("pulsar://localhost:6650")
    val adminUrl = args.headOption.map(domain => s"http://${domain}:8080").getOrElse("http://localhost:8080")
    // Set up the test environment
    val experiment = PulsarClientSetup.setup("testing", "part2", adminUrl, brokerUrl) match {
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
            val payload = TestPayload(name = "first-test-message", 0)
            producer.send(payload)
          }
          _ <- {
            val payload = TestPayload(name = "second-test-message", 0)
            producer.send(payload)
          }
          _ <- {
            val payload = TestPayload(name = "third-test-message", 0)
            producer.send(payload)
          }
        } yield {

          val source = new PulsarPekkoSource(consumer)
          val processor = new SimpleMessageProcessor(producer, "parallel-consumer", 5000, 500, logger, system, ec)

          // Configure stream parallelism
          val parallelism = StreamParallelism(
            processingParallelism = 2, // Process 2 messages concurrently
            acknowledgmentParallelism = 6
          )

          // Create the source with parallel processing
          val (_, streamCompletionFuture) = PekkoStreamGenerator
            .createStreamFromSourceAndProcessingSteps(
              consumer = consumer,
              source = source,
              parallelism = parallelism,
              processor = processor,
              metrics = metrics
            )
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

          source.drainAndShutdown(Future.successful(())).map { _ => system.terminate() }
        }
      case Failure(exception) =>
        logger.info(s"Failed to set up test environment: ${exception.getMessage}")
        system.terminate()
    }

    Await.result(experiment, 2.minute)
  }
}
