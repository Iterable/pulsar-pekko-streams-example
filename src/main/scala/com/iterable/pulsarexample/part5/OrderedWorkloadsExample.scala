package com.iterable.pulsarexample.part5

import com.iterable.pulsarexample.part2.StreamParallelism
import com.iterable.pulsarexample.part4.BasicDiscoveryService
import com.iterable.pulsarexample.part4.Workload
import com.iterable.pulsarexample.part4.WorkloadManagementService
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pulsar.client.admin.PulsarAdmin
import WorkloadManagementService.creationFlow
import com.iterable.pulsarexample.part3.PermittedTaskExecutorImpl
import com.iterable.pulsarexample.util.PulsarClient
import com.iterable.pulsarexample.util.PulsarClientSetup
import com.iterable.pulsarexample.util.PulsarClientWrapper
import com.iterable.pulsarexample.util.PulsarProducer
import com.iterable.pulsarexample.util.TestNamespace
import PulsarClientWrapper.ProducerConfig
import com.iterable.pulsarexample.util.NoOpCollector
import com.iterable.pulsarexample.util.ProcessedMessage
import zio.Clock
import zio.Runtime
import zio.Unsafe

import java.util.concurrent.Executors
import scala.concurrent.Await
import scala.concurrent.duration.DurationLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.jdk.FutureConverters.*

object OrderedWorkloadsExample {

  val logger = Logger("MultipleStreamsExample")

  val testTenant = "testing"
  val testNamespace = "part5"

  def createMessages(producer: PulsarProducer[TestPayloadOrdered], messages: Seq[(String, String)])(implicit
    executionContext: ExecutionContext
  ): Future[Unit] = {
    Future.sequence {
      messages.map { message =>
        val (name, key) = message
        producer.newMessage().key(key).value(TestPayloadOrdered(name, key)).sendAsync().asScala
      }
    }.map(_ => ())
  }

  /**
   * Handles graceful shutdown of resources
   */
  def performGracefulShutdown(
    producers: Set[PulsarProducer[TestPayloadOrdered]],
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
      new PermittedTaskExecutorImpl[() => Future[ProcessedMessage[TestPayloadOrdered]], ProcessedMessage[
        TestPayloadOrdered
      ]](
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
            TestPayloadOrdered.schema
          )
          producer2 <- PulsarClientWrapper.createProducer(
            client,
            ProducerConfig(topic2, Some("producer2")),
            TestPayloadOrdered.schema
          )
          producer3 <- PulsarClientWrapper.createProducer(
            client,
            ProducerConfig(topic3, Some("producer3")),
            TestPayloadOrdered.schema
          )

          _ <- createMessages(
            producer1,
            Seq(
              ("lion", "wild-cat"),
              ("tiger", "wild-cat"),
              ("calico", "domesticated-cat"),
              ("siamese", "domesticated-cat")
            )
          )
          _ <- createMessages(
            producer2,
            Seq(
              ("apple", "fruit-tree"),
              ("cherry", "fruit-tree"),
              ("raspberry", "fruit-bramble"),
              ("blackberry", "fruit-bramble")
            )
          )

          producerMap = Map(
            topic1 -> producer1,
            topic2 -> producer2
          )
          processor = new OrderedMessageProcessor(
            producerMap,
            "processor",
            4000,
            2000,
            logger,
            system,
            system.dispatcher,
            logIndividualMessages = false,
            allowFailures = false
          )

          discoveryService = new BasicDiscoveryService()

          runtime: Runtime[Any] = Runtime.default.mapEnvironment(env => env.add(materializer).add(Clock))

          streamCreationFlow =
            OrderedStreamGenerator.createOrderedConsumerFlow(
              client,
              TestPayloadOrdered.schema,
              queueingExecutor,
              processor,
              new NoOpCollector,
              runtime
            )(
              ec,
              materializer
            )

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
          val workload2 = Workload("fruit-workload", topic2, StreamParallelism(5, 5))

          logger.info("No consumption because no workloads are registered")

          Thread.sleep(5000)

          logger.info("Adding another workload (cats)")

          discoveryService.setNewWorkloads(Set(workload1, workload2))

          Thread.sleep(30000)

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

    Await.result(experiment, 1.minute)
  }
}
