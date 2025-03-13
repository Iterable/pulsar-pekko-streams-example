package com.iterable.pulsarexample.util

import org.apache.pulsar.client.api.CompressionType
import org.apache.pulsar.client.api.ConsumerBuilder
import org.apache.pulsar.client.api.HashingScheme
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.MessageRoutingMode
import org.apache.pulsar.client.api.ProducerBuilder
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionMode
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.client.api.TypedMessageBuilder
import org.apache.pulsar.client.api.Producer as JavaProducer
import org.apache.pulsar.client.api.Consumer as JavaConsumer
import org.apache.pulsar.client.api.PulsarClient as JavaPulsarClient
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.client.impl.auth.AuthenticationToken

import scala.concurrent.Future
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters.*
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters.*
import scala.concurrent.duration.*
import scala.util.Try

implicit class VoidFutureConverter(cf: CompletableFuture[Void]) {

  /**
   * Converts a CompletableFuture[Void] to Future[Unit]
   *
   * @param ec
   *   The execution context for the Future
   * @return
   *   A Scala Future[Unit]
   */
  def asScalaUnitFuture(implicit ec: ExecutionContext): Future[Unit] = {
    cf.asScala.map(_ => ())(ec)
  }
}

/**
 * Scala wrapper for Pulsar Producer providing better type safety and a more idiomatic API
 */
class PulsarProducer[T](underlying: JavaProducer[T]) {
  def send(message: T): Future[MessageId] =
    underlying.sendAsync(message).asScala
  def newMessage(): TypedMessageBuilder[T] = underlying.newMessage()
  def close(): Unit = underlying.close()
  def closeAsync()(implicit executionContext: ExecutionContext): Future[Unit] =
    underlying.closeAsync().asScalaUnitFuture
  def flush(): Unit = underlying.flush()
  def getTopic: String = underlying.getTopic
  def isConnected: Boolean = underlying.isConnected
}

/**
 * Scala wrapper for Pulsar Consumer providing better type safety and a more idiomatic API
 */
class PulsarConsumer[T](underlying: JavaConsumer[T]) {

  def receive(): Message[T] = underlying.receive()
  def receiveAsync(): Future[Message[T]] = underlying.receiveAsync().asScala
  def acknowledge(message: Message[T]): Unit = underlying.acknowledge(message)
  def acknowledgeAsync(message: Message[T])(implicit executionContext: ExecutionContext): Future[Unit] =
    underlying.acknowledgeAsync(message).asScalaUnitFuture
  def negativeAcknowledge(message: Message[T]): Unit = underlying.negativeAcknowledge(message)
  def close(): Unit = underlying.close()
  def closeAsync()(implicit executionContext: ExecutionContext): Future[Unit] =
    underlying.closeAsync().asScalaUnitFuture
  def isConnected: Boolean = underlying.isConnected
  def seek(messageId: MessageId): Unit = underlying.seek(messageId)
  def seekAsync(messageId: MessageId)(implicit executionContext: ExecutionContext): Future[Unit] =
    underlying.seekAsync(messageId).asScalaUnitFuture
}

/**
 * Scala wrapper for Pulsar client providing better type safety and a more idiomatic API
 */
class PulsarClient(underlying: JavaPulsarClient) {
  def newProducer[T](schema: Schema[T]): ProducerBuilder[T] = underlying.newProducer(schema)
  def newConsumer[T](schema: Schema[T]): ConsumerBuilder[T] = underlying.newConsumer(schema)
  def close(): Unit = underlying.close()
  def closeAync()(implicit executionContext: ExecutionContext): Future[Unit] = underlying.closeAsync().asScalaUnitFuture
}

/**
 * A wrapper around the Pulsar client providing utilities for Scala-friendly producer and consumer creation
 */
object PulsarClientWrapper {
  // Default configuration values remain the same
  val DefaultServiceUrl = "pulsar://localhost:6650"
  val DefaultMaxLookupRequests = 50000
  val DefaultMaxConcurrentLookupRequests = 5000
  val DefaultMaxOperationTimeoutMs = 30000
  val DefaultConnectionTimeoutMs = 10000
  val DefaultStartingBackoffIntervalMs = 100
  val DefaultMaxBackoffIntervalMs = 60000
  val DefaultEnableTransaction = false
  val DefaultTlsHostnameVerificationEnable = false
  val DefaultNumIoThreads = 1
  val DefaultNumListenerThreads = 1
  val DefaultAllowTlsInsecureConnection = true

  case class ClientConfig(
    serviceUrl: String = DefaultServiceUrl,
    authToken: Option[String] = None,
    maxLookupRequests: Int = DefaultMaxLookupRequests,
    maxConcurrentLookupRequests: Int = DefaultMaxConcurrentLookupRequests,
    operationTimeout: Duration = DefaultMaxOperationTimeoutMs.millis,
    connectionTimeout: Duration = DefaultConnectionTimeoutMs.millis,
    startingBackoffInterval: Duration = DefaultStartingBackoffIntervalMs.millis,
    maxBackoffInterval: Duration = DefaultMaxBackoffIntervalMs.millis,
    enableTransaction: Boolean = DefaultEnableTransaction,
    tlsHostnameVerificationEnable: Boolean = DefaultTlsHostnameVerificationEnable,
    numIoThreads: Int = DefaultNumIoThreads,
    numListenerThreads: Int = DefaultNumListenerThreads,
    allowTlsInsecureConnection: Boolean = DefaultAllowTlsInsecureConnection
  )

  /**
   * Creates a Pulsar client with the specified configuration
   */
  def createClient(config: ClientConfig): Try[PulsarClient] = {
    Try {
      val builder = JavaPulsarClient
        .builder()
        .serviceUrl(config.serviceUrl)
        .maxLookupRequests(config.maxLookupRequests)
        .maxConcurrentLookupRequests(config.maxConcurrentLookupRequests)
        .operationTimeout(config.operationTimeout.toMillis.toInt, TimeUnit.MILLISECONDS)
        .connectionTimeout(config.connectionTimeout.toMillis.toInt, TimeUnit.MILLISECONDS)
        .startingBackoffInterval(config.startingBackoffInterval.toMillis.toInt, TimeUnit.MILLISECONDS)
        .maxBackoffInterval(config.maxBackoffInterval.toMillis.toInt, TimeUnit.MILLISECONDS)
        .enableTransaction(config.enableTransaction)
        .enableTlsHostnameVerification(config.tlsHostnameVerificationEnable)
        .ioThreads(config.numIoThreads)
        .listenerThreads(config.numListenerThreads)
        .allowTlsInsecureConnection(config.allowTlsInsecureConnection)

      config.authToken.foreach { token =>
        builder.authentication(new AuthenticationToken(token))
      }

      new PulsarClient(builder.build())
    }
  }

  case class ProducerConfig(
    topic: String,
    name: Option[String] = None,
    sendTimeout: Duration = Duration(30, TimeUnit.SECONDS),
    maxPendingMessages: Int = 1000,
    blockIfQueueFull: Boolean = false,
    messageRoutingMode: MessageRoutingMode = MessageRoutingMode.RoundRobinPartition,
    compressionType: CompressionType = CompressionType.NONE,
    hashingScheme: HashingScheme = HashingScheme.JavaStringHash,
    batchingEnabled: Boolean = true,
    batchingMaxMessages: Int = 1000
  )

  case class ConsumerConfig(
    topics: List[String],
    subscriptionName: String,
    subscriptionType: SubscriptionType = SubscriptionType.Shared,
    subscriptionMode: SubscriptionMode = SubscriptionMode.Durable,
    receiverQueueSize: Int = 1000,
    acknowledgementsGroupTime: Duration = Duration(100, TimeUnit.MILLISECONDS),
    negativeAckRedeliveryDelay: Duration = Duration(10, TimeUnit.SECONDS),
    name: Option[String] = None
  )

  /**
   * Creates a producer with the specified configuration
   */
  def createProducer[T](
    client: PulsarClient,
    config: ProducerConfig,
    schema: Schema[T]
  )(implicit executionContext: ExecutionContext): Future[PulsarProducer[T]] = {
    val builder = client
      .newProducer(schema)
      .topic(config.topic)
      .sendTimeout(config.sendTimeout.toMillis.toInt, TimeUnit.MILLISECONDS)
      .maxPendingMessages(config.maxPendingMessages)
      .blockIfQueueFull(config.blockIfQueueFull)
      .messageRoutingMode(config.messageRoutingMode)
      .compressionType(config.compressionType)
      .hashingScheme(config.hashingScheme)
      .enableBatching(config.batchingEnabled)
      .batchingMaxMessages(config.batchingMaxMessages)

    config.name.foreach(builder.producerName)

    builder.createAsync().asScala.map { producer => new PulsarProducer(producer) }
  }

  /**
   * Creates a consumer with the specified configuration
   */
  def createConsumer[T](
    client: PulsarClient,
    config: ConsumerConfig,
    schema: Schema[T]
  )(implicit executionContext: ExecutionContext): Future[PulsarConsumer[T]] = {

    println("entered consumer creation")
    val builder = client
      .newConsumer(schema)
      .topics(config.topics.asJava)
      .subscriptionName(config.subscriptionName)
      .subscriptionType(config.subscriptionType)
      .subscriptionMode(config.subscriptionMode)
      .receiverQueueSize(config.receiverQueueSize)
      .acknowledgmentGroupTime(config.acknowledgementsGroupTime.toMillis, TimeUnit.MILLISECONDS)
      .negativeAckRedeliveryDelay(config.negativeAckRedeliveryDelay.toMillis, TimeUnit.MILLISECONDS)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)

    config.name.foreach(builder.consumerName)

    builder.subscribeAsync().asScala.map { consumer =>
      new PulsarConsumer(consumer)
    }
  }
}
