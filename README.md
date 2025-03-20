# Pulsar Pekko Consumer

An example demonstrating how to build out a highly parallel workload management system using Scala, Pekko, and Apache Pulsar.

**Pointers** and **Disclaimers**

1. The best way to run this system is with IntelliJ or VS Code using the debugger to see how things happen.
2. If you don't understand something feel free to add additional logging to the system to see what is going on.
3. This is not production ready so expect some bugs around shutdown and message cleanup.
4. If you have issues running tests repeatedly clear out the pulsar container by running `docker-commpose down && docker-compose up`

## Resources

1. [Pekko Streams QuickStart](https://pekko.apache.org/docs/pekko/1.0/stream/stream-quickstart.html)
2. [Pekko Streams Basics](https://pekko.apache.org/docs/pekko/1.0/stream/stream-flows-and-basics.html#basics-and-working-with-flows)
3. [CleverCloud pulsar4s Repo - Pekko Source implementation](https://github.com/CleverCloud/pulsar4s)
4. [How Pulsar Works](https://jack-vanlightly.com/blog/2018/10/2/understanding-how-apache-pulsar-works)
5. [Multi Topic Consumers](https://jack-vanlightly.com/?offset=1589457102345)
6. [Pulsar Summit Talk on How Iterable uses Pulsar](https://www.youtube.com/watch?v=mbq5vnagzWk)
7. [Reactive Streams Interop](https://pekko.apache.org/docs/pekko/1.1/stream/reactive-streams-interop.html)


## Overview of what is built here:

We'll build the consumer out in parts working from a single consumer
working as fast as possible.

Stages to building out a workload management system:

1. Creating a Pekko Source that wraps a Pulsar Consumer and allows us to apply back pressure to Pulsar.
2. Building a Pekko Stream generator that takes a Pulsar Consumer and gives us a Stream for processing messages on that consumer.
   - The stream applies back pressure via concurrency limits to processing and acknowledging messages.
   - We run a single stream to show that consuming and back pressure work.
3. Expanding to multiple workloads using a global permitting system each stream requests bandwidth from.
   - We build a permitting system using another Pekko Stream
   - We hook that permitting system into our workload stream generator so each stream obeys global concurrency limits.
   - We run multiple streams in an example to show this system working.
4. Managing our streams dynamically by allowing the creation and deletion of workloads/streams dynamically.
   - We build a basic management service using more Pekko streams that can add and remove workloads/streams independently.
   - We create a basic discovery service that we can check for new and removed workloads.
   - We run an example where we add and remove workloads to show this working.
5. We introduce ZIO to provide bounded key based ordering because Pekko Streams does not it.
   - Pulsar has a Key-Shared consumer that guarantees every message with the same key goes to the same consumer.
   - We need to support that in our streaming solution but Pekko Streams does not support an unbounded number of keys.
   - To avoid rewriting everything, we use Reactive streams to substitute part of the Pekko Streams implementation with ZIO Streams.
   - We convert our PulsarPekkoSource to a ZIO Stream, process using ZIO, and then convert back.

### Examples

Each example runs for a fixed amount of time and publish/consume from a topic in a loop. The examples
build on each other each demonstrating a different part of the system we are building.

## Setup and Running Purely With Docker

Requirements:
   - Docker and docker-compose

How to Run:

1. Build an image to run all  `docker build -t pekko-example .`
2. Start pulsar in one terminal `docker-compose down && docker-compose up`
3. Run an example in another terminal: `docker run --network pulsar-network -it pekko-example sbt "runMain com.iterable.pulsarexample.part3.MultipleWorkloadsExample pulsar"`

Running each part:

- Part 2: `docker run --network pulsar-network -it pekko-example sbt "runMain com.iterable.pulsarexample.part2.SingleWorkloadExample pulsar"`
- Part 3: `docker run --network pulsar-network -it pekko-example sbt "runMain com.iterable.pulsarexample.part3.MultipleWorkloadsExample pulsar"`
- Part 4: `docker run --network pulsar-network -it pekko-example sbt "runMain com.iterable.pulsarexample.part4.WorkloadDiscoveryExample pulsar"`
- Part 5: `docker run --network pulsar-network -it pekko-example sbt "runMain com.iterable.pulsarexample.part5.OrderedWorkloadsExample pulsar"`

## Setup and Running in IntelliJ or VS Code

Requirements:
   - Java 21
   - Scala 3.3
   - SBT
   - Docker and docker-compose

How to run:
1. Open a terminal window and run `docker-compose up` to start the Pulsar container.
2. Open a separate tab and run `sbt compile` to compile the project. 
3. Run via the command line with `sbt "runMain com.iterable.pulsarexample.part3.MultipleWorkloadsExample"`
4. Or open in an IDE and run the mains directly. You only need to provide a domain name as an argument if you're running
pulsar on a network besides localhost.

Running each part

- Part 2: `sbt "runMain com.iterable.pulsarexample.part2.SingleWorkloadExample"`
- Part 3: `sbt "runMain com.iterable.pulsarexample.part3.MultipleWorkloadsExample"`
- Part 4: `sbt "runMain com.iterable.pulsarexample.part4.WorkloadDiscoveryExample"`
- Part 5: `sbt "runMain com.iterable.pulsarexample.part5.OrderedWorkloadsExample"`

## Part 1 - A Pekko source built for a consumer

We need to wrap a Pulsar consumer with a Pekko Source so we have something to pull messages from.

### Key Parts

1. Push/Pull model
   - a downstream step has capacity and sends a "pull" request to the source
   - the source receives the pull request and calls `consumer.receiveAsync`
   - when a message is consumed the Source "pushes" the message out to the downstream step
   - the downstream step begins processing and will send another pull request if it still has capacity
2. Backpressure and concurrency limits are applied at each stage in a Source or Flow. These are all downstream of the Source.

All of this means that concurrency limits naturally bubble up to earlier stages in the stream as backpressure. So we
only consume messages when we have capacity to process them.

This default behavior is extremely useful for managing workloads.


## Part 2 - Building a consumer Flow with a Pekko Source

With a wrapped consumer we now need to build a Stream processing messages from the Source. The stream has to limit
concurrency and apply backpressure which the Source already supports.

### Key Parts

1. `PekkoStreamGenerator` given a source create a Flow with two steps with different backpressure and concurrency limits.
  - Concurrency limits can be applied at each step in the flow.
  - The most difficult part is not creating and running this stream, but shutting it down cleanly. The order of the shutdown
    is important. We need to drain the consumer first and then let the stream empty naturally before shutting down the producers and client. 

### Running the example

1. Run `docker-compose down && docker-compose up` to start the Pulsar container.
2. Run `sbt "runMain com.iterable.pulsarexample.part2.SingleWorkloadExample"` to run the example on the command line.
3. Or build a docker image with `docker build -t pekko-example .` and run the image with `docker run --network pulsar-network -it pekko-example sbt "runMain com.iterable.pulsarexample.part2.SingleWorkloadExample pulsar"`

### Example

1. We create a stream for processing messages from a topic and publishing 
2. We create a stream that reads from the topic with limits on concurrency.
3. We create a limited source that cannot process all of the messages on the topic at the same time.
4. We let the stream run for awhile and log out the messages in progress at any one time.

## Part 3 - Managing multiple streams at the same time

We need a way to globally limit the streams to not overwhelm the system, but we need to maintain the independence of
each workload.

1. We need to apply concurrency limits and backpressure in the order of precedence. So first we need to apply
the global concurrency limit with backpressure *and then* the stream level permit limit with backpressure.
2. We introduce a permit system that all streams submit tasks to.
3. That permit system returns callbacks that when completed signal different things about the task.
4. We apply our stream level limits to the callbacks.

### Key Parts

1. `PermittedTaskExecutor` - A wrapped pekko stream that applies a global concurrency limit to a stream of tasks.
   - `PermittedTaskExecutor.globalPermitSystem` is the core where we maintain of queue of tasks to start
2. `MultipleConsumerFlow.composeSourceWithProcessingSteps` - We add a step to every workload stream submitting the task to the permitted executor.
   - The task executor returns a `Future[Future[T]]` where the outer Future completes when the task is queued and the inner Future completes when the task is done.
   - We only queue one (or only a few) tasks at a time since queueing is instant
   - We then apply our stream level processing limit on the inner future

### Running the example

1. Run `docker-compose down && docker-compose up` to start the Pulsar container.
2. Run `sbt "runMain com.iterable.pulsarexample.part3.MultipleWorkloadsExample"` to run the example on the command line.
3. Or build a docker image with `docker build -t pekko-example .` and run the image with `docker run --network pulsar-network -it pekko-example sbt "runMain com.iterable.pulsarexample.part3.MultipleWorkloadsExample pulsar"`

### Example

The example has a lot of boilerplate and can be confusing to read.

In a nutshell we have a few important parts:

1. Proving the global permitting system is working:
   - We create a PermittedTaskExecutor that logs out how many permits it has granted. 
2. Proving the global permitting system is actively processing messages and not just granting permits:
   - We create a shared message processor that logs out how many tasks are in progress at any one time.
3. We create three workloads each cycling through a different set of messages on different topics.
   - These workloads have 18 in progress messages, so much more than we can progress.
4. We run the example for a minute and then shutdown.

What you see when we run the example is there are many queued tasks and only a few running tasks. That corresponds to
the number of messages being actively processed.

To see more swap the `NoOpCollector` with the `SimpleMessageCollector` which will spit out all of the acks/nacks going on.

## Part 4 - Adding the ability to discover new workloads and remove old workloads to the consumer

### Key Parts

1. In Part 3 we saw that streams run independently from each other and we can hook into streams whenever we want.
2. `WorkloadManagementService`: We formalize this by building two new pekko streams a `creationQueue` stream and `deletionQueue` stream.
3. `WorkloadManagementService`: We also add a stream checking for new and removed workloads every X seconds and then adding them
to the creation and deletion streams as necessary.
4. The key part here is that once a stream is created it is completely independent. So we can have a service creating and discarding
a stream yet know that Pekko will manage whatever we create for us. This allows us to dynamically modify running workloads
with just a few lines of boilerplate.

### Running the example

1. Run `docker-compose down && docker-compose up` to start the Pulsar container.
2. Run `sbt "runMain com.iterable.pulsarexample.part4.WorkloadDiscoveryExample"` to run the example on the command line.
3. Or build a docker image with `docker build -t pekko-example .` and run the image with `docker run --network pulsar-network -it pekko-example sbt "runMain com.iterable.pulsarexample.part4.WorkloadDiscoveryExample pulsar"`

### Example

The discovery service looking for new workloads is one we manually add workloads to just to simplify the example.

1. Create three workloads
   1. Cats: messages about cats
   2. Fruit: messages about fruit
   3. Colors: messages about colors
2. We wait for a few seconds to show no consuming happens until we tell it to
3. **Start** We start processing Cats and then wait
4. **Start** We start processing Fruit and then wait
5. **Delete** We stop processing Cats and then Wait
6. **Both** We start both Cats and Colors and stop Fruit to show multiple changes being applied at once.


## Part 5 - Adding ordering

**Disclaimer:** this is complicated, it takes a bit to understand what is going on.

Now we're going to show an advanced way of extending our implementation using other streaming libraries. This is very important
because you may want different characteristics for different libraries.

We're going to add ZIO by modifying just 1 - yes only 1, function.

### Why we need ZIO

Pulsar has an advanced feature called [Key_Shared Subscriptions](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#key_shared),
which provides an advanced form of ordering.

> Messages are delivered in a distribution across consumers and message with same key or same ordering key are delivered to only one consumer. No matter how many times the message is re-delivered, it is delivered to the same consumer. When a consumer connected or disconnected will cause served consumer change for some key of message.

This is a powerful feature that we want, however, Pekko Streams does not support it.

Pekko Streams offers a `groupBy` method that does create streams grouped by key; however, it doesn't support a potentially
infinite number of dynamic keys. Either you potentially lose stream elements or you run out of memory.

From the Pekko Streams docs:

> Note: If **allowClosedSubstreamRecreation is set to true** substream completion and incoming elements are subject to race-conditions. If elements arrive for a stream that is in the process of closing these elements might get lost.
> Note: If **allowClosedSubstreamRecreation is set to false** (default behavior) the operators keeps track of all keys of streams that have already been closed. If you expect an infinite number of keys this can cause memory issues. Elements belonging to those keys are drained directly and not send to the substream.

ZIO however, does support this kind of dynamic grouping. So somehow we need to make ZIO handle our processing without rewriting everything.

### Reactive Streams Specification 

The way we incorporate ZIO and Pekko together is via the [Reactive Streams Specification](https://github.com/reactive-streams/reactive-streams-jvm).

The specification defines a standard set of interfaces for streams written in a JVM based language.

1. A Pekko Source becomes a Publisher
2. A Pekko Flow becomes a Processor (which is both a Subscriber and a Publisher)
3. A Pekko Sink becomes a Subscriber

Many centrally important libraries implement this specification including:

1. Pekko Streams
2. ZIO Streams
3. Play Framework
4. Spring Integration and Pivotal Project Reactor 
5. RxJava from Netflix
6. AWS SDK 2.0
7. Many major databases implement reactive streams: MongoDB, Cassandra, Elasticsearch, Kafka, RMQ, etc.

### Key Parts

We leave the PulsarPekkoSource, PermittedTaskExecutor, and WorkloadManagemenntService alone. We are
**focused only on the inner stream**. 

1. We create a ZIO Runtime that uses our existing Pekko Materializer to run its streams.
2. We pass this Runtime to a new processing stream generator.
3. Within that generator we do the following major steps:
   1. We create a ZIO Stream from the PulsarPekkoSource by first converting to a reactive stream
   2. We call `ZStreams.groupBy` on the stream to group by key and we set the parallelism per key to 1.
   3. We apply a ZIO Semaphore to these grouped streams to maintain our workload level concurrency limits and backpressure
   4. We trigger the new ZStream to run and ignore the result.
   5. We return a ShutdownHook transparently.

### Example

1. We create two workloads each with two keys:
   1. cats - wild and domesticated
   2. fruits - trees and bushes
2. We create a management service for our workloads that uses our new generator function to generate ordered streams.
3. We start the management service and then start both workloads.
   1. The workloads run with a limit of 1 message per key in parallel which we log to prove.
