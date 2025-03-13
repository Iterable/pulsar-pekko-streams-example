package com.iterable.pulsarexample.part1

import com.iterable.pulsarexample.util.PulsarConsumer
import org.apache.pekko.stream.stage.*
import org.apache.pekko.stream.{Attributes, Outlet, SourceShape}
import org.apache.pulsar.client.api.Message

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Promise}
import org.apache.pekko.Done

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/*
 * Note: this is based off the pulsar4s library now maintained by CleverCloud
 *
 * See: https://github.com/CleverCloud/pulsar4s
 */

/**
 * We need these hooks to control how the stream is shutdown and cleaned up.
 *
 * Pekko does have a stop callback but in some ways its better for us to take direct control.
 */
trait ShutdownHook {

  def complete(): Future[Done]

  def shutdown(): Future[Done]

  def drainAndShutdown[S](streamCompletion: Future[S])(implicit ec: ExecutionContext): Future[S]
}

/**
 * For all of the boilerplate in here only two things matter:
 *
 *   1. [[PulsarSourceLogic.onPull()]] Get a request from a downstream handler to consume a message 2.
 *      [[PulsarSourceLogic.receiveCallback]] Attempt to consume a message from pulsar and if we do consume it
 *      successfully push the message to the downstream handler. 3.
 */
class PulsarSourceLogic[T](
  consumer: PulsarConsumer[T],
  out: Outlet[Message[T]],
  shape: SourceShape[Message[T]]
) extends GraphStageLogic(shape)
    with OutHandler
    with ShutdownHook {

  implicit def ec: ExecutionContext = materializer.executionContext

  private val receiveCallback = getAsyncCallback[Try[Message[T]]] {
    case Success(m) => {
      push(out, m)
    }
    case Failure(e) => {
      failStage(e)
    }
  }

  override def onPull(): Unit = {
    consumer.receiveAsync().onComplete(receiveCallback.invoke)
  }

  private val stopped: Promise[Done] = Promise()
  private val stopCallback: AsyncCallback[Unit] = getAsyncCallback(_ => completeStage())

  setHandler(
    out,
    this
  )

  override def preStart(): Unit = {
    // Make sure that on shutdown we wait before cleaning up.
    stopped.future.onComplete { _ =>
      // Schedule to stop after a delay to give unacked messages time to finish
      materializer.scheduleOnce(1.minute, () => (close(): Unit))
    }
  }

  override def postStop(): Unit = {
    // Cleanup logic, e.g., closing Pulsar consumer
    stopped.success(Done)
  }

  private def close(): Future[Done] = {
    consumer.closeAsync().map(_ => Done)
  }

  def shutdown(): Future[Done] = {
    for {
      _ <- complete()
      _ <- close()
    } yield Done
  }

  def complete(): Future[Done] = {
    stopCallback.invoke(())
    stopped.future
  }

  def drainAndShutdown[S](streamCompletion: Future[S])(implicit ec: ExecutionContext): Future[S] = {
    complete()
      .flatMap(_ => streamCompletion)
      .transformWith { resultTry =>
        shutdown().transform {
          case Success(_) => resultTry
          case Failure(e) => resultTry.flatMap(_ => Failure(e))
        }
      }
  }
}

class PulsarPekkoSource[T](consumer: PulsarConsumer[T])
    extends GraphStageWithMaterializedValue[SourceShape[Message[T]], ShutdownHook] {

  private val out = Outlet[Message[T]]("pulsar.out")
  private lazy val logic = new PulsarSourceLogic(consumer, out, shape)

  override def shape: SourceShape[Message[T]] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, ShutdownHook) = {
    (logic, logic)
  }

  def drainAndShutdown[S](streamCompletion: Future[S])(implicit ec: ExecutionContext): Future[S] = {
    logic.drainAndShutdown(streamCompletion)
  }
}
