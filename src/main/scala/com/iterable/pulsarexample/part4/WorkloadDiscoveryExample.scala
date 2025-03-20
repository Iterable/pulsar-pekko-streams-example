package com.iterable.pulsarexample.part4

import com.iterable.pulsarexample.part2.StreamParallelism
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pulsar.client.admin.PulsarAdmin
import WorkloadManagementService.creationFlow
import com.iterable.pulsarexample.part3.MapMessageProcessor
import com.iterable.pulsarexample.part3.PermittedTaskExecutorImpl
import com.iterable.pulsarexample.util.PulsarClient
import com.iterable.pulsarexample.util.PulsarClientSetup
import com.iterable.pulsarexample.util.PulsarClientWrapper
import com.iterable.pulsarexample.util.PulsarProducer
import com.iterable.pulsarexample.util.TestNamespace
import com.iterable.pulsarexample.util.TestPayload
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

object WorkloadDiscoveryExample {

  val logger = Logger("MultipleStreamsExample")

  val testTenant = "testing"
  val testNamespace = "part4"

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
        globalPermitLimit = 20,
        maxTasksQueued = 20,
        maxTasksWaiting = 20,
        wrappedFunctionToExecute = f => f(),
        taskExecutionContext = taskExecutionContext,
        addShutdownHook = true,
        reportingInterval = 250.seconds
      )

    val brokerUrl = args.headOption.map(domain => s"pulsar://${domain}:6650").getOrElse("pulsar://localhost:6650")
    val adminUrl = args.headOption.map(domain => s"http://${domain}:8080").getOrElse("http://localhost:8080")

    val experiment = PulsarClientSetup.setup(testTenant, testNamespace, adminUrl, brokerUrl) match {
      case Success((client, admin, namespace)) =>
        logger.info("Successfully setup Pulsar client")

        val topic1 = s"persistent://${testTenant}/${testNamespace}/first"
        val topic2 = s"persistent://${testTenant}/${testNamespace}/second"
        val topic3 = s"persistent://${testTenant}/${testNamespace}/third"

        // Bootstrap example
        for {
          // Create producer and processor
          producer1 <- PulsarClientWrapper.createProducer(
            client,
            ProducerConfig(topic1, Some("producer1")),
            TestPayload.schema
          )
          producer2 <- PulsarClientWrapper.createProducer(
            client,
            ProducerConfig(topic2, Some("producer2")),
            TestPayload.schema
          )
          producer3 <- PulsarClientWrapper.createProducer(
            client,
            ProducerConfig(topic3, Some("producer3")),
            TestPayload.schema
          )

          _ <- createMessages(
            producer1,
            Seq("lion", "tiger", "panther"),
            TestPayload.apply
          )
          _ <- createMessages(
            producer2,
            Seq("wolf", "coyote", "fox"),
            TestPayload.apply
          )
          _ <-
            createMessages(
              producer3,
              Seq("eagle", "falcon", "hawk", "owl"),
              TestPayload.apply
            )

          producerMap = Map(
            topic1 -> producer1,
            topic2 -> producer2,
            topic3 -> producer3
          )
          processor = new MapMessageProcessor(
            producerMap,
            "processor",
            4000,
            150000,
            logger,
            system,
            system.dispatcher,
            allowFailures = false
          )

          discoveryService = new BasicDiscoveryService()

          streamCreationFlow =
            creationFlow(client, TestPayload.schema, queueingExecutor, processor, new NoOpCollector)(ec, materializer)
          managementService = WorkloadManagementService.build(
            client,
            discoveryService,
            system,
            system.dispatcher,
            materializer,
            streamCreationFlow
          )

          _ = managementService.startWorkloadUpdaterFlow()
        } yield {
          val workload1 = Workload("cats-workload", topic1, StreamParallelism(5, 5))
          val workload2 = Workload("dogs-workload", topic2, StreamParallelism(5, 5))
          val workload3 = Workload("birds-workload", topic3, StreamParallelism(5, 5))

          logger.info("Nothing is running because we haven't 'discovered' any workloads yet")

          Thread.sleep(5000)

          logger.info("Starting a single workload (cats)")

          discoveryService.setNewWorkloads(Set(workload1))

          Thread.sleep(10000)

          logger.info("Starting a second workload (dogs)")

          discoveryService.setNewWorkloads(Set(workload1, workload2))

          Thread.sleep(10000)

          logger.info("Stopping the first workload (cats)")

          discoveryService.setNewWorkloads(Set(workload2))

          Thread.sleep(10000)

          logger.info("Stopping the second workload (dogs) AND Staring both the first workload (cats) and a new third workload (birds)")

          discoveryService.setNewWorkloads(Set(workload1, workload3))

          Thread.sleep(10000)

          managementService.shutdown().map { _ =>
            for {
              _ <- producer1.closeAsync()
              _ <- producer2.closeAsync()
              _ <- producer3.closeAsync()
              _ <- client.closeAync()
            } yield {
              system.terminate()
            }
          }
        }

      case Failure(exception) =>
        logger.error(s"Failed to setup Pulsar client: ${exception.getMessage}")
        system.terminate()
    }

    Await.result(experiment, 3.minute)
  }
}
