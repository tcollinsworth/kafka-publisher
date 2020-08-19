# KafkaPublisher

Intentionally best-effort publishing - but tries really hard not to lose messages.
Possible to lose or duplicate a message on crash or error.
If kafka errors or is unavailable, falls back to writing messages to the filesystem until kafka available again.

Queues messages in memory.
Worker attempts to publish to kafka topic with fixed retries.
When kafka publish retries exhausted, switch to writing messages to filesystem until kafka available again.
Tries to write to filesystem with limited retries before logging error and discarding.
Periodically retries connecting to kafka in background.
When successful, immediately switches back to publishing to kafka.

Partitioning:

The node-rdkafka lib, or more correctly librdkafka does not compute partitions from key exactly the same as
kafka-console-publisher kafka/utils.partitioner, murmur2-partitioner, or no-kafka libs, see [issue](https://github.com/Blizzard/node-rdkafka/issues/616).
This library currently utilizes the murmur2-partitioner library to specify the partition overriding the
librdkafka partitioner when a key is specified on publish.

Future:

When kafka recovers, start background job to load messages from filesystem into kafka.
Messages written to filesystem may be published to kafka out-of-order with respect to current messages being published.

Log and discard message if

  * bad key - (null, undefined, Boolean, Symbol) must be (string, number, object)
  * bad value - not JSON object
  * message too large - written to '.largeMesg.fallback.log'

## Requirements

Node 10+

## Getting started

```console
npm i kafka-publisher --save
```

# Usage

```javascript
import { KafkaPublisher } from 'kafka-publisher'

const options = {
  // comma delimited list of seed brokers
  connectionString: '127.0.0.1:9092',
  defaultTopic: 'someTopicName'
}
const kp = new KafkaPublisher(options)

const key = 'someKey'
const message = { foo: 'bar', bar: 'baz' }

// queuing messages is synchronous
// asynchronously send/persist/retry in background
// if retries exhausted, falls-back to appending to a file,
// when kafka available, continues publishing to kafka

// queue a message
kp.queue(key, message)
```

# Methods

   * `KafkaPublisher(options)` - constructor, creates publisher and client
   * `init()` - initializes kafka, connecting to broker, returns promise, but should not await if utilizing fallback
   * `shutdown()` - closes the kafka connection, returns promise
   * `queue(key, message[, topic])` - queue a message for publishing to kafka, the defaultTopic will be used unless topic is provided
   * `queueMessages([{key, message[, topic]}, ...])` - queue an array of messages for publishing to kafka, the defaultTopic will be used unless topic is provided
   * `getStatistics()` - gets all statistics, should be exposed via a REST endpoint
   * `resetStatistics()` - resets all statistics, should be exposed via a REST endpoint

### Options

The only required option is 'connectionString'. Other options generally have reasonable defaults.

To avoid having to specify the topic on every message, set a defaultTopic.
If the topic is passed with a message, it overrides the defaultTopic.

The option fallback.directory specifies where the ...fallback.log will append messages while kafka is unavailable.
For a pool of servers, it is recommended this be a non-ephemeral filesystem mounted on all the nodes.
For example NFS on kubernetes/openshift or Amazon Web Services EFS https://aws.amazon.com/efs/


```javascript
const defaultOptions = {
  // deprecated, legacy, from prior no-kafka config
  connectionString: 'FIXME', // default process.env.KAFKA_URL || '27.0.0.1:9092',

  defaultTopic: undefined,

  producer: {
    // 'client.id': 'kafka-publisher', // default rdkafka
    // 'metadata.broker.list': '127.0.0.1:9092', // default none - auto-populated from legacy connectionString
    // request.required.acks: 1, // default 1, -1 all ISR
    // request.timeout.ms: 5000, // default 5 sec
    // message.timeout.ms is the risk window of losing message if kafka becomes unavailable before timeouts and writing to fallback
    // if kafka loss is detected via error event, fallback persistence will occur immediately upon detection
    'message.timeout.ms': 10100, // default 300000 (5 min) - deliviery timeout, ideally (request.timeout.ms x message.send.max.retries + margin)
    // 'compression.codec': 'none', // default inherit, none, gzip, snappy, lz4, inherit
    // 'compression.level': -1, // default -1, (-1 .. 12)
    // socket.timeout.ms: 60000, // default 1 min
    // socket.keepalive.enable: false, // default false
    // socket.nagle.disable: false, // default false
    // batch.num.messages: 10000, // default 10000 (1 .. 1000000)
    // queue.buffering.max.messages: 100000, //default 100K
    // queue.buffering.max.kbytes: 1048576, // default 1M
    // 'queue.buffering.max.ms': 0, // default 0 (0 .. 900000)
    // message.send.max.retries: 2, // default 2
    // retry.backoff.ms: 100, // default 100 (1 .. 300000)
    dr_cb: true,
    event_cb: true,
    // debug: 'all', // disable for production
  },

  producerPollIntervalMs: 50, // Polls the producer for delivery reports or other events to be transmitted via the emitter and handling disconnections and reconnection

  // consecutive error threshold till reconnect is initiated
  // cleared on successful delivery report (kafka ack) and on transition to kafka ready
  consecutiveKafkaErrorCntReconnectThreshold: 10,
  kafkaReadyOrErrorOrTimeoutMs: 60000, // 1 min
  kafkaReadyOrErrorOrTimeoutPollMs: 100, // 100 ms

  // connect retry options
  // min/max timeout is not a timeout on try, they bound time till next retry after failed try
  retryOptions: {
    retries: null, // not strictly required, however disables creating default retry table
    // retries: 10000, // 10K ~2 months - creates a retry schedule for all retries (rediculous, why not computing) 8 9's causes FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory
    forever: true, // use this instead of retries or it will create a lookup table for all retries wasting cycles and memory
    factor: 2,
    minTimeout: 1000, // 1 sec
    maxTimeout: 10000, // 10 sec
    randomize: true,
  },

  // fallback defaults - where to write to filesystem
  fallback: {
    // instanceId: <uuid>, //passed by/from kafka-publisher on construction
    enabled: true,

    directory: 'kafkaFallbackLogs', // non-ephemeral filesystem mount, shared by all nodes
    retryOptions: {
      retries: 2, // not strictly required, however disables creating default retry table
      // retries: 10000, // 10K ~2 months - creates a retry schedule for all retries (rediculous, why not computing) 8 9's causes FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory
      // forever: true, // use this instead of retries or it will create a lookup table for all retries wasting cycles and memory
      factor: 2,
      minTimeout: 100, // 0.1 sec
      maxTimeout: 2000, // 2 sec
      randomize: true,
    },
  },

  // pino logging options
  logging: {
    name: 'kafka-publisher',
    // enabled: true,
    // level: 'debug', // default 'info' //comment out or set to 'info'
    // timestamp: true,
    prettyPrint: process.env.NODE_ENV === 'DEBUG' || false,
    useLevelLabels: true,
  },
}
```

# Reverted to es5

Update index.js and the following to switch to es6 supported by node 10.

   * package.json
   * .bablerc
   * .eslintrc
   * .gitignore
