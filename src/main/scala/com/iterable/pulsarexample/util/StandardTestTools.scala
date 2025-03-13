package com.iterable.pulsarexample.util

import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Message

/**
 * Represents the result of message processing and determines acknowledgment behavior
 */
sealed trait ProcessingResult

object ProcessingResult {
  case object ProcessSuccess extends ProcessingResult
  case class ProcessFailure(e: Exception) extends ProcessingResult
}

/**
 * A message envelope that carries both the processed result and the original message for acknowledgment purposes
 *
 * @param result
 *   The processed result indicating success/failure/retry
 * @param originalMessage
 *   The original Pulsar message for acknowledgment
 * @tparam T
 *   The type of the original message
 * @tparam R
 *   The type of the processed result
 */
case class ProcessedMessage[T](
  result: ProcessingResult,
  originalMessage: Message[T]
)

/**
 * Trait for collecting metrics about message processing
 */
trait MetricsCollector {
  def recordProcessingTime(message: String, nanos: Long): Unit
  def recordAckTime(message: String, nanos: Long): Unit
  def recordNackTime(message: String, nanos: Long): Unit
  def recordBatchSize(size: Int): Unit
  def recordError(): Unit
  def recordSuccess(): Unit
  def recordRetry(): Unit
}

/**
 * Simple metrics collector for demonstration purposes
 */
class SingleStreamCollector(logger: Logger) extends MetricsCollector {
  private var successCount = 0
  private var errorCount = 0
  private var retryCount = 0

  def recordProcessingTime(m: String, nanos: Long): Unit = {
    logger.info(s"[${nanos / 1000000.0}ms] ${m}")
  }

  def recordAckTime(m: String, nanos: Long): Unit = logger.info(s"[${nanos / 1000000.0}ms] ${m}")

  def recordNackTime(m: String, nanos: Long): Unit = logger.info(s"[${nanos / 1000000.0}ms] ${m}")

  def recordBatchSize(size: Int): Unit = logger.info(s"Batch size: $size")

  def recordError(): Unit = {
    errorCount += 1
  }

  def recordSuccess(): Unit = {
    successCount += 1
  }

  def recordRetry(): Unit = {
    retryCount += 1
  }
}

class NoOpCollector extends MetricsCollector {

  def recordProcessingTime(m: String, nanos: Long): Unit = {
    ()
  }

  def recordAckTime(m: String, nanos: Long): Unit = ()

  def recordNackTime(m: String, nanos: Long): Unit = ()

  def recordBatchSize(size: Int): Unit = ()

  def recordError(): Unit = ()

  def recordSuccess(): Unit = ()

  def recordRetry(): Unit = ()
}
