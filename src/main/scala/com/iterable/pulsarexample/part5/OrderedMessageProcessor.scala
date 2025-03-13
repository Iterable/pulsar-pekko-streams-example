package com.iterable.pulsarexample.part5

import com.iterable.pulsarexample.part3.LimitedMessageProcessor
import com.iterable.pulsarexample.util.ProcessingResult
import com.iterable.pulsarexample.util.PulsarProducer
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Cancellable
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.SchemaInfo
import play.api.libs.json.Format
import play.api.libs.json.Json
import ProcessingResult.ProcessFailure
import ProcessingResult.ProcessSuccess

import scala.jdk.FutureConverters.*
import scala.concurrent.duration.DurationLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Random

case class TestPayloadOrdered(name: String, key: String, numPublishes: Int)

object TestPayloadOrdered {
  def apply(name: String, key: String): TestPayloadOrdered = TestPayloadOrdered(name, key, 0)

  def apply(message: TestPayloadOrdered): TestPayloadOrdered =
    TestPayloadOrdered(message.name, message.key, message.numPublishes + 1)

  implicit val format: Format[TestPayloadOrdered] = Json.format[TestPayloadOrdered]

  val schema: Schema[TestPayloadOrdered] = new Schema[TestPayloadOrdered] {
    override def encode(message: TestPayloadOrdered): Array[Byte] = format.writes(message).toString().getBytes

    override def getSchemaInfo: SchemaInfo = null

    override def decode(bytes: Array[Byte], schemaVersion: Array[Byte]): TestPayloadOrdered =
      Json.parse(bytes).as[TestPayloadOrdered]

    override def clone(): Schema[TestPayloadOrdered] = this
  }

}

/**
 * To simplify things have one message processor for all streams.
 *
 * Same as the processor from parts 3 and 4, just publishes back to Pulsar with a key.
 */
class OrderedMessageProcessor(
  producers: Map[String, PulsarProducer[TestPayloadOrdered]],
  name: String,
  sleep: Long,
  checkInterval: Long,
  logger: Logger,
  system: ActorSystem,
  executionContext: ExecutionContext,
  logIndividualMessages: Boolean = true,
  allowFailures: Boolean = true
) extends LimitedMessageProcessor[TestPayloadOrdered] {

  val inProgress: scala.collection.concurrent.TrieMap[String, TestPayloadOrdered] =
    new scala.collection.concurrent.TrieMap[String, TestPayloadOrdered]()

  val cancellable: Cancellable = system.scheduler.scheduleAtFixedRate(
    initialDelay = 0.seconds,
    interval = checkInterval.millis
  )(() => {

    val inProgressReport =
      inProgress.values.groupBy(_.key).map((key, messages) => s"Key: ${key} has ${messages.size} in Progress").toSeq
    logger.info(s"[${name}] Total in progress: ${inProgress.size}.\t${inProgressReport.mkString(",\t")}")
  })(executionContext)

  system.registerOnTermination {
    cancellable.cancel()
    logger.info("Cancelling in progress monitor")
  }

  // Make sure we're using the globally limited context
  def processMessage(
    message: Message[TestPayloadOrdered]
  )(implicit executionContext: ExecutionContext): Future[ProcessingResult] = {

    val payload = message.getValue

    inProgress.put(payload.name, message.getValue)

    if (logIndividualMessages) {
      logger.info(
        s"${name}: ${payload.name} publish #${payload.numPublishes}"
      )
    }

    Future {
      Thread.sleep(sleep)

      if (allowFailures && Random.nextInt(10) == 0) {
        logger.error(s"Simulating failure for message: ${payload.name} publish #${payload.numPublishes}")
        inProgress.remove(payload.name)
        Future.successful(ProcessFailure(new Exception("Simulating processing failure")))
      } else {
        val topicName = message.getTopicName.stripSuffix("-partition-0")
        // Sleep for X milliseconds to simulate processing time
        val producer = producers(topicName)

        producer
          .newMessage()
          .key(message.getKey)
          .value(TestPayloadOrdered(payload))
          .sendAsync()
          .asScala
          .map { _ =>
            inProgress.remove(payload.name)
            ProcessSuccess
          }
      }
    }.flatten
  }
}
