# KafkaPublisher

Intentionally best-effort publishing.
Possible to lose or duplicate a message on crash or error.
If kafka errors or is unavailable, writes messages to the filesystem until kafka available again.

Queues messages in memory.
Worker attempts to publish to kafka topic with fixed retries.
When kafka publish retries exhausted, switch to writing messages to filesystem until kafka available again.
Tries to write to filesystem with limited retries before logging error and discarding.
Periodically retries publishing to kafka in background.
When successful, immediately switches back to publishing to kakfa.

Future:

When kafka recovers, start background job to load messages from filesystem into kafka.
Messages written to filesystem will be published to kafka out-of-order with respect to current messages being published.

Log and discard message if

  * bad key - (null, undefined, Boolean, Symbol) must be (string, number, object)
  * bad value - not JSON object
  * message too large

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

// queue one message
kp.queue(key, message)

// OR

// queue array of messages
// topic is optional and uses defaultTopic if not provided
// [
//   { key, message, topic },
//   ...
// ]
kp.queueMessages(messageArray)
```

# Methods

   * `KafkaPublisher(options)` - constructor, creates publisher and client
   * `init()` - initializes kafka, connecting to broker, returns promise, but should not await if utilizing fallback
   * `end()` - closes the kafka connection, return promise
   * `queue(key, message[, topic])` - queue a message for publishing to kafka, the defaultTopic will be used unless topic is provided
   * `queueMessages([{key, message[, topic]}, ...])` - queue a messages for publishing to kafka, the defaultTopic will be used unless topic is provided
   * `getStatistics()` - gets all statistics, should be exposed via a REST endpoint
   * `resetStatistics()` - resets all statistics, should be exposed via a REST endpoint

### Options

The only required option is 'connectionString'. Other options generally have reasonable defaults.

To avoid having to specify the topic on every message, set a defaultTopic.
If the topic is passed with a message, it overrides the defaultTopic.

The option fallback.directory specifies where the fallback-<UUIDv4>.log will append messages while kafka is unavailable.
For a pool of servers, it is recommended this be a non-ephemeral filesystem mounted on all the nodes.
For example NFS on kubernetes/openshift or Amazon Web Services EFS https://aws.amazon.com/efs/


```javascript
const defaultOptions = {
  //producer defaults
  defaultTopic: undefined,
  requiredAcks: -1, // all in-sync replicas
  timeout: 30000,
  partitioner: new Kafka.DefaultPartitioner(),
  retries: {
    attempts: 7, // 2.8 sec @ 7 retries min 100, max 1000 - 100+200+300+400+500+600+700=2800
    delay: {
      min: 100, // 100 ms
      max: 1000, // 1 sec
    },
  },
  batch: {
    size: 16384,
    maxWait: 100 // 100 ms
  },
  codec: Kafka.COMPRESSION_NONE,

  //client
  clientId: 'no-kafka-client',
  connectionString: 'FIXME', // comma delimited kafka seed broker list '127.0.0.1:9092,...'
  default ssl: {
    cert: process.env.KAFKA_CLIENT_CERT,
    key: process.env.KAFKA_CLIENT_CERT_KEY,
    // secureProtocol: 'TLSv1_method',
    rejectUnauthorized: false,
    // ciphers: 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA',
    ca: process.env.KAFKA_CLIENT_CA
  },
  asyncCompression: true,
  brokerRedirection: false,
  reconnectionDelay: {
    min: 1000,
    max: 10000 // default 1000
  },
  default logger: {
    logLevel: 5,
  },

  // init retry options
  retryOptions = {
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
    directory: 'kafkaFallbackLogs', // recommend a non-ephemeral filesystem mount, shared by all nodes
    retryOptions: {
      retries: 5, // not strictly required, however disables creating default retry table
      // retries: 10000, // 10K ~2 months - creates a retry schedule for all retries (rediculous, why not computing) 8 9's causes FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory
      //forever: true, // use this instead of retries or it will create a lookup table for all retries wasting cycles and memory
      factor: 2,
      minTimeout: 100, // 0.1 sec
      maxTimeout: 2000, // 2 sec
      randomize: true,
    },
  },
}
```

# Reverted to es5

Update index.js and the following to switch to es6 supported by node 10.
