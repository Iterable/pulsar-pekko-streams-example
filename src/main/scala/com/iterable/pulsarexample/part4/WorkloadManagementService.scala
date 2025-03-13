package com.iterable.pulsarexample.part4

import com.iterable.pulsarexample.part1.PulsarPekkoSource
import com.iterable.pulsarexample.part1.ShutdownHook
import com.iterable.pulsarexample.part2.StreamParallelism
import com.iterable.pulsarexample.part3.LimitedMessageProcessor
import com.iterable.pulsarexample.part3.PermittedTaskExecutor
import com.typesafe.scalalogging.Logger
import org.apache.pekko.Done
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Cancellable
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pulsar.client.api.Schema
import com.iterable.pulsarexample.part3.PermittedPekkoStreamGenerator.createStreamWithProcessingAndReturnShutdownHook
import com.iterable.pulsarexample.util.PulsarClient
import com.iterable.pulsarexample.util.PulsarClientWrapper
import com.iterable.pulsarexample.util.PulsarConsumer
import PulsarClientWrapper.ConsumerConfig
import com.iterable.pulsarexample.util.MetricsCollector
import com.iterable.pulsarexample.util.ProcessedMessage

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

case class Workload(workloadName: String, topic: String, streamParallelism: StreamParallelism) {
  override def equals(other: Any): Boolean = {
    other match {
      case w: Workload => workloadName == w.workloadName
      case _           => false
    }
  }
}

case class WorkloadReport(requestedWorkloads: Set[Workload], existingWorkloads: Set[Workload]) {

  def workloadsToStart: Set[Workload] = requestedWorkloads.diff(existingWorkloads)

  def workloadsToDelete: Set[Workload] = existingWorkloads.diff(requestedWorkloads)

}

/**
 * Basic discovery service which is essentially a stub.
 *
 * You might use a service mesh or centralized leasing service to manage workloads and this would just be a client on
 * top of them.
 */
trait WorkloadDiscoveryService {

  def getWorkloadChangeRequests(): WorkloadReport
}

class BasicDiscoveryService extends WorkloadDiscoveryService {

  private val logger = Logger("WorkloadDiscoveryService")

  private var existingWorkloads: Set[Workload] = Set.empty

  private var newSetOfWorkloads: Set[Workload] = Set.empty

  override def getWorkloadChangeRequests(): WorkloadReport = {
    synchronized {
      val report = WorkloadReport(newSetOfWorkloads, existingWorkloads)
      if (newSetOfWorkloads.nonEmpty) {
        logger.info(s"Retrieved new set of workloads ${newSetOfWorkloads.map(_.workloadName)}")
        existingWorkloads = newSetOfWorkloads
        newSetOfWorkloads = Set.empty
      }
      report
    }

  }

  def setNewWorkloads(newWorkloads: Set[Workload]): Unit = {
    synchronized {
      newSetOfWorkloads = newWorkloads
    }
  }
}

object BasicDiscoveryService {
  def apply(): BasicDiscoveryService = new BasicDiscoveryService()
}

class WorkloadManagementService(
  discoveryService: WorkloadDiscoveryService,
  system: ActorSystem,
  materializer: Materializer,
  implicit val executionContext: ExecutionContext,
  createAStreamForAWorkload: Flow[Workload, (String, ShutdownHook), NotUsed]
) {

  private val logger = Logger("WorkloadManager")

  private val runningWorkload: scala.collection.concurrent.Map[String, ShutdownHook] =
    scala.collection.concurrent.TrieMap.empty

  // Only start the management service once
  private val started = new AtomicBoolean(false)

  /**
   * Use Pekko Stream queues, when we have a workload to create we add it to the queue and the stream will pick it up
   * and create the workload.
   *
   * We add checks for whether the workload was already created or not and emit a kill switch so we can kill this when
   * it finishes.
   */
  private val (workloadStartQueue, workloadStartQueueShutdownHook) = Source
    .queue[Workload](100, OverflowStrategy.backpressure, 10)
    // Try not to start workloads twice.
    // This isn't sufficient but is good enough for a demo.
    .filterNot { workload =>
      runningWorkload.contains(workload.workloadName)
    }
    .via(createAStreamForAWorkload)
    // Mark a workload as started
    .map { case (workloadName, shutdownHook) =>
      runningWorkload.put(workloadName, shutdownHook)
    }
    .viaMat(KillSwitches.single)(Keep.both)
    .toMat(Sink.ignore)(Keep.left)
    .run()(materializer)

  /**
   * Use Pekko Stream queues for deleting workloads as well. Offer a workload to be deleted and emit a shutdown hook so
   * we can kill the stream when the service is shutting down.
   */
  private val (workloadStoppingQueue, workloadStoppingQueueShutdownHook) = Source
    .queue[Workload](100, OverflowStrategy.backpressure, 10)
    .map { workload =>
      runningWorkload.remove(workload.workloadName) match {
        case Some(shutdownHook) =>
          shutdownHook
            .drainAndShutdown(Future.successful(()))
            .map(_ => workload)
        case None =>
          Future.successful(workload)
      }
    }
    .viaMat(KillSwitches.single)(Keep.both)
    .toMat(Sink.ignore)(Keep.left)
    .run()(materializer)

  private val tickCancellable = new AtomicReference[Cancellable](Cancellable.alreadyCancelled)

  /**
   * Create a Pekko Stream that on a fixed schedule looks for workloads to start and stop.
   *
   * It queries the discovery service for changes to requested workloads and queues those changes.
   */
  def startWorkloadUpdaterFlow(): Unit = {

    if (!started.get()) {
      // On a fixed schedule look for new workloads to run
      val cancelTick = Source
        .tick(0.seconds, 5.seconds, NotUsed)
        .map { _ =>
          val report = discoveryService.getWorkloadChangeRequests()
          if (report.requestedWorkloads.nonEmpty) {
            val workloadsToStart = report.workloadsToStart
            val workloadsToDelete = report.workloadsToDelete
            logger.info(
              s"WorkloadManager Found ${report.requestedWorkloads.size} Starting ${workloadsToStart
                  .map(_.workloadName)} and Deleting ${workloadsToDelete.map(_.workloadName)}"
            )

            // Add workloads to start and stop to the queue.
            // This does not actually start them, only makes sure they are queued to be started.
            val workloadsToStartFt = Future.sequence(workloadsToStart.map { workload =>
              workloadStartQueue.offer(workload)
            })

            val workloadsToStopFt = Future.sequence(workloadsToDelete.map { workload =>
              workloadStoppingQueue.offer(workload)
            })

            for {
              _ <- workloadsToStartFt
              _ <- workloadsToStopFt
            } yield {
              ()
            }
          } else {
            ()
          }
        }
        .toMat(Sink.ignore)(Keep.left)
        .run()(materializer)

      tickCancellable.set(cancelTick)
    }
  }

  def shutdown(): Future[Done] = {

    if (started.get()) {

      tickCancellable.get().cancel()
      workloadStartQueueShutdownHook.shutdown()
      workloadStoppingQueueShutdownHook.shutdown()

      for {
        _ <- workloadStartQueue.watchCompletion()
        _ <- workloadStoppingQueue.watchCompletion()
      } yield {
        Done
      }
    } else {
      Future.successful(Done)
    }
  }
}

object WorkloadManagementService {

  def build[T](
    client: PulsarClient,
    discoveryService: WorkloadDiscoveryService,
    system: ActorSystem,
    executionContext: ExecutionContext,
    materializer: Materializer,
    streamCreationFlow: Flow[Workload, (String, ShutdownHook), NotUsed]
  ): WorkloadManagementService = {
    new WorkloadManagementService(
      discoveryService,
      system,
      materializer,
      executionContext,
      streamCreationFlow
    )
  }

  /**
   * In previous parts the consumer already existed for us. In this case we must create the consumer and then the stream
   * serving a particular workload.
   */
  def creationFlow[T](
    client: PulsarClient,
    schema: Schema[T],
    queueingTaskExecutor: PermittedTaskExecutor[() => Future[ProcessedMessage[T]], ProcessedMessage[T]],
    processor: LimitedMessageProcessor[T],
    metrics: MetricsCollector
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
              .createConsumer(client, ConsumerConfig(List(workload.topic), "test-subscription"), schema)
        } yield {
          (workload, consumer)
        }
      }
      // Follow the same steps as in part 3 to create a new stream for the workload
      .map { case (workload: Workload, consumer: PulsarConsumer[T]) =>
        val source = new PulsarPekkoSource(consumer)

        // Method from part 3
        val shutdownHook: ShutdownHook = createStreamWithProcessingAndReturnShutdownHook(
          consumer = consumer,
          source = source,
          parallelism = workload.streamParallelism,
          processor = processor,
          metrics = metrics,
          queueingTaskExecutor = queueingTaskExecutor
        )

        (workload.workloadName, shutdownHook)
      }
  }
}
