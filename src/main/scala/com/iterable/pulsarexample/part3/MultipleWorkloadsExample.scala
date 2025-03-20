package com.iterable.pulsarexample.part3

import com.iterable.pulsarexample.part1.ShutdownHook
import com.iterable.pulsarexample.part2.StreamParallelism
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import PermittedPekkoStreamGenerator.buildMultiConsumerFlow
import com.iterable.pulsarexample.util.PulsarClient
import com.iterable.pulsarexample.util.PulsarClientSetup
import com.iterable.pulsarexample.util.PulsarClientWrapper
import com.iterable.pulsarexample.util.PulsarConsumer
import com.iterable.pulsarexample.util.PulsarProducer
import com.iterable.pulsarexample.util.TestNamespace
import com.iterable.pulsarexample.util.TestPayload
import PulsarClientWrapper.ConsumerConfig
import PulsarClientWrapper.ProducerConfig
import com.iterable.pulsarexample.util.NoOpCollector
import com.iterable.pulsarexample.util.ProcessedMessage

import java.util.concurrent.Executors
import scala.concurrent.Await
import scala.concurrent.duration.DurationLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

object MultipleWorkloadsExample {

  val logger = Logger("MultipleStreamsExample")

  val testTenant = "testing"
  val testNamespace = "part3"

  def createProducerConsumer[T](topic: String, sleep: Long, client: PulsarClient, schema: Schema[T])(implicit
    executionContext: ExecutionContext
  ): Future[(PulsarProducer[T], PulsarConsumer[T])] = {

    // Create producer configuration
    val producerConfig = ProducerConfig(
      topic = topic,
      name = Some("unique-producer"),
      batchingEnabled = false
    )

    // Create consumer configuration
    val consumerConfig = ConsumerConfig(
      topics = List(topic),
      subscriptionName = "test-subscription",
      subscriptionType = SubscriptionType.Shared,
      receiverQueueSize = 100
    )

    for {
      producer <- PulsarClientWrapper.createProducer(client, producerConfig, schema)
      consumer <- PulsarClientWrapper.createConsumer(client, consumerConfig, schema)
    } yield {
      (producer, consumer)
    }
  }

  def createMessages[T](producer: PulsarProducer[T], messages: Seq[String], messageCreator: String => T)(implicit
    executionContext: ExecutionContext
  ): Future[Unit] = {
    Future.sequence {
      messages.map { message =>
        producer.send(messageCreator(message))
      }
    }.map(_ => ())
  }

  /**
   * Handles graceful shutdown of resources
   */
  def performGracefulShutdown(
    producers: Set[PulsarProducer[TestPayload]],
    client: PulsarClient,
    admin: PulsarAdmin,
    namespace: TestNamespace
  )(implicit executionContext: ExecutionContext): Future[Unit] = {

    for {
      _ <- Future.sequence(producers.map(_.closeAsync()))
      close <- client.closeAync()
    } yield {
      close
    }
  }

  def main(args: Array[String]): Unit = {

    // Create actor system and materializer for Pekko Streams
    implicit val system: ActorSystem = ActorSystem("PulsarPekkoExample")
    implicit val materializer: Materializer = Materializer(system)

    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))

    val taskExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())

    val queueingExecutor =
      new PermittedTaskExecutorImpl[() => Future[ProcessedMessage[TestPayload]], ProcessedMessage[TestPayload]](
        system = system,
        materializer = materializer,
        globalPermitLimit = 5,
        maxTasksQueued = 20,
        maxTasksWaiting = 10,
        wrappedFunctionToExecute = f => f(),
        taskExecutionContext = taskExecutionContext,
        addShutdownHook = true,
        reportingInterval = 1.seconds
      )

    // Here just for the sake of brevity
    val parallelism = StreamParallelism(5, 5)

    val brokerUrl = args.headOption.map(domain => s"pulsar://${domain}:6650").getOrElse("pulsar://localhost:6650")
    val adminUrl = args.headOption.map(domain => s"http://${domain}:8080").getOrElse("http://localhost:8080")
    val experiment = PulsarClientSetup.setup("testing", "part3", adminUrl, brokerUrl) match {
      case Success((client, admin, namespace)) =>
        logger.info("Successfully setup Pulsar client")

        for {
          (shutdownHooks, producers) <- setupMultiStreamExperiment(system, queueingExecutor, client)
        } yield {

          // Run the example for 30 seconds and then shut it down.
          Thread.sleep(30000)

          // Cleanly start shutdown of the streams and wait for each consumer to shut down
          val shutdownResults = Future.sequence(shutdownHooks.map { _.drainAndShutdown(Future.successful(())) })

          // Normally we'd wait for each stream to drain by monitoring the global permit system
          // but for the sake of the example stop after a delay.
          Thread.sleep(2000)

          shutdownResults.map { _ =>
            logger.info("Shutting down...")
            performGracefulShutdown(producers, client, admin, namespace)
          }.map { _ =>
            system.terminate()
          }

        }

      case Failure(exception) =>
        logger.error(s"Failed to setup Pulsar client: ${exception.getMessage}")
        system.terminate()
    }

    Await.result(experiment, 2.minute)
  }

  private def setupMultiStreamExperiment(
    system: ActorSystem,
    queueingTaskExecutor: PermittedTaskExecutor[() => Future[ProcessedMessage[TestPayload]], ProcessedMessage[
      TestPayload
    ]],
    client: PulsarClient
  )(implicit
    executionContext: ExecutionContext,
    materializer: Materializer
  ): Future[(Set[ShutdownHook], Set[PulsarProducer[TestPayload]])] = {

    val topic1 = s"persistent://${testTenant}/${testNamespace}/first"
    val topic2 = s"persistent://${testTenant}/${testNamespace}/second"
    val topic3 = s"persistent://${testTenant}/${testNamespace}/third"

    for {
      (producer1, consumer1) <- createProducerConsumer(topic1, 1000, client, TestPayload.schema)
      (producer2, consumer2) <- createProducerConsumer(topic2, 1000, client, TestPayload.schema)
      (producer3, consumer3) <- createProducerConsumer(topic3, 1000, client, TestPayload.schema)

      collector = new NoOpCollector

      // Create processor for all messages that can track # of messages in progress at any one time
      producerMap = Map(
        topic1 -> producer1,
        topic2 -> producer2,
        topic3 -> producer3
      )
      sharedProcessor =
        new MapMessageProcessor(
          producerMap,
          "processor",
          1000,
          1000,
          logger,
          system,
          executionContext,
          logIndividualMessages = true,
          allowFailures = false
        )

      parallelism = StreamParallelism(5, 5)
      // This is not a stream of streams. The response is actually a set of shutdown hooks.
      // We generate each stream and allow Pekko's Materializer to manage them.
      streamOfShutdownHooks = {
        // For each consumer create a stream and return the means for shutting down that stream cleanly.
        Source(
          List(
            (consumer1, parallelism, sharedProcessor),
            (consumer2, parallelism, sharedProcessor),
            (consumer3, parallelism, sharedProcessor)
          )
        ).via(buildMultiConsumerFlow(collector, queueingTaskExecutor))
          .toMat(Sink.fold(Set.empty[ShutdownHook])(_ + _))(Keep.both)
          .run()
      }

      /*
       * Create messages to start the example. We have 15 messages but only allow 10 tasks to run at once.
       */
      _ <- createMessages(producer1, Seq("lion", "tiger", "panther", "leopard", "lynx", "jaguar"), TestPayload.apply)
      _ <- createMessages(
        producer2,
        Seq("wolf", "coyote", "jackal", "dingo", "fox", "dhole"),
        TestPayload.apply
      )
      _ <-
        createMessages(producer3, Seq("falcon", "eagle", "hawk", "vulture", "osprey", "kite", "owl"), TestPayload.apply)

      // Get the set of shutdown hooks for each stream
      shutdownHooks <- streamOfShutdownHooks._2
    } yield {
      (shutdownHooks, producerMap.values.toSet)
    }
  }
}
