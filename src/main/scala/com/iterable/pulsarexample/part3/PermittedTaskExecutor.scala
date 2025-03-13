package com.iterable.pulsarexample.part3

import com.typesafe.scalalogging.Logger
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Cancellable
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.stream.ActorAttributes
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.QueueOfferResult
import org.apache.pekko.stream.Supervision
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success

/**
 * A generic task executor that queues tasks and executes them in parallel. The executor is backed by a Pekko Stream
 * queue and a pool of worker threads. The executor is designed to be used with a single type of task and result.
 *
 * Pekko Streams supports backpressuring through queues so when the queue of tasks is full we will not accept any more
 * tasks.
 *
 * We use a queueing system to allow us to smooth starting new tasks instead of back-pressuring immediately when there
 * are no permits left.
 *
 * If there are issues with this permitter consuming fairly you can add a prioritization layer on top for different
 * kinds of workloads. Typically it is not necessary.
 */
trait PermittedTaskExecutor[T, V] {

  protected val logger = Logger("QueueingTaskExecutor")

  def system: ActorSystem

  final def queuingTaskContext: ExecutionContext = system.dispatcher

  implicit def materializer: Materializer

  def globalPermitLimit: Int

  def maxTasksQueued: Int

  def maxTasksWaiting: Int

  def wrappedFunctionToExecute: T => Future[V]

  def taskExecutionContext: ExecutionContext

  def addShutdownHook: Boolean

  protected val activeTasks = new AtomicInteger(0)
  protected val queuedTasks = new AtomicInteger(0)
  protected val concurrentOffers = new AtomicInteger(0)

  // On initialization add a shutdown hook to close the Pekko Stream
  if (addShutdownHook) {
    CoordinatedShutdown(system)
      .addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, s"${this.getClass.getName} queue stop") { () =>
        shutdown()
      }
  }

  // Next initialize the Pekko Stream starting tasks
  val (globalPermitSystem, done): (SourceQueueWithComplete[QueueEntry], Future[Done]) = {
    /*
     * OverflowStrategy.backpressure: if we hit the max queue size, queue up to maxConcurrentOffers offer futures.
     */
    val queueSource =
      Source.queue[QueueEntry](maxTasksQueued, OverflowStrategy.backpressure, maxTasksWaiting)

    queueSource
      // Apply the global concurrency limit
      .mapAsyncUnordered(globalPermitLimit) { entry =>

        // Make sure the task is executed in a separate thread pool from Pekko's main one
        val taskFuture = Future.delegate(wrappedFunctionToExecute(entry.task))(taskExecutionContext)
        activeTasks.incrementAndGet()
        queuedTasks.decrementAndGet()

        entry.promise
          .completeWith(taskFuture)
          .future
          .transform { result =>
            activeTasks.decrementAndGet()
            Success(Done)
          }(queuingTaskContext)
      }
      .toMat(Sink.ignore)(Keep.both)
      .withAttributes(ActorAttributes.supervisionStrategy {
        // If there's an error close the stream
        case e: Throwable =>
          logger.error(s"Task executor $getClass shutting down due to unhandled exception.", e)
          Supervision.Stop
      })
      .run()
  }

  private def isCompleted: Boolean = globalPermitSystem.watchCompletion().isCompleted

  @volatile private[this] var closed: Boolean = false

  def shutdown(): Future[Done] = {
    closed = true
    // When shutting down the app, wait for the completion of the stream to make sure we process all tasks
    globalPermitSystem.complete()
    done
  }

  final class QueueEntry(myTask: T, myPromise: Promise[V]) {
    private val enqueuedTime = Instant.now().toEpochMilli
    private val dequeued: AtomicBoolean = new AtomicBoolean(false)

    def task: T = { myTask }

    def promise: Promise[V] = { myPromise }
  }

  /**
   * Submit a task to the executor. The top level [[Future]] represents whether the task was successfully accepted into
   * the queue The nested [[Future]] is completed after the task is executed
   * @return
   *   If the task was submitted successfully: a successful [[Future]] containing a [[Future]] that'll be completed when
   *   the task executes Otherwise: a failed [[Future]], with the [[Throwable]] containing information about why the
   *   task couldn't be submitted
   */
  def submit(task: T): Future[Future[V]] = {

    // The Future of this Promise is what other Streams are waiting on
    val promise = Promise[V]()
    val entry = new QueueEntry(task, promise)

    concurrentOffers.incrementAndGet()

    globalPermitSystem
      // Queue the task that needs to execute
      .offer(entry)
      // Create the Future[Future] construct where the inner future is the callback completed when the task finishes
      // and the outer Future is completed when the task enters the queue.
      .transform {
        case Success(QueueOfferResult.Enqueued) =>
          Success(promise.future)
        case Success(result) =>
          Failure(new RuntimeException(s"Failed to enqueue task: $result"))
        case Failure(e) =>
          Failure(e)
      }(queuingTaskContext)
      .andThen {
        case Success(taskFt) =>
          queuedTasks.incrementAndGet()
          concurrentOffers.decrementAndGet()
        case _ =>
          concurrentOffers.decrementAndGet()
      }(queuingTaskContext)
  }
}

class PermittedTaskExecutorImpl[T, V](
  override val system: ActorSystem,
  override val materializer: Materializer,
  override val globalPermitLimit: Int,
  override val maxTasksQueued: Int,
  override val maxTasksWaiting: Int,
  override val wrappedFunctionToExecute: T => Future[V],
  override val taskExecutionContext: ExecutionContext,
  override val addShutdownHook: Boolean = true,
  val reportingInterval: FiniteDuration
) extends PermittedTaskExecutor[T, V] {

  val cancellable: Cancellable = system.scheduler.scheduleAtFixedRate(
    initialDelay = FiniteDuration(0, "seconds"),
    interval = reportingInterval
  )(() => {
    logger.info(s"[Global Task Executor] Running Tasks: ${activeTasks.get()}, Queued Tasks: ${queuedTasks
        .get()}, Offered: ${concurrentOffers.get()}")
  })(system.dispatcher)

  if (addShutdownHook) {
    CoordinatedShutdown(system)
      .addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, s"${this.getClass.getName} queue stop") { () =>
        cancellable.cancel()
        Future.successful(Done)
      }
  }
}
