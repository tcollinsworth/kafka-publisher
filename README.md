# KafkaPublisher

Intentionally best-effort publishing.
Possible to lose or duplicate a message on crash or error.
If kafka errors or unavailable, write messages to filesystem until kafka recovers or errors resolved.

Queues messages in memory.
Worker attempts to publish to kafka topic with fixed retries.
When kafka publish retries exhausted, switch to writing messages to filesystem.
Continues to retry kafka in background.
When kafka recovers switches to writing to kakfa.

Future
When kafka recovers, start background job to load messages from filesystem to kafka.
Messages written to filesystem will be likely be published to kafka out-of-order with respect to current messages.

Log and discard message if

  * bad key - (null, undefined, Boolean, Symbol) must be (string, number, object)
  * key too large
  * bad value - not JSON object
  * value too large

## Requirements

Node 8+

## Getting started

```console
npm i kafka-publisher --save
```

# Usage

```javascript
import { KafkaPublisher, publish, getStatistics } from 'kafka-publisher'

const options = { connectionString: '127.0.0.1:9092', defaultTopic: 'someTopicName' } // comma delimited list of seed brokers
const kp = new KafkaPublisher(options)

const messageKey = 'someKey'
const messageBody = { foo: 'bar', bar: 'baz' }

kp.queue(messageKey, messageBody) // NOT async, queues synchronously and asynchronously persists/retries in background or falls-back to appending to a file
```

### Options

All supported options

The only required option is 'connectionString'.

All other options generally have reasonable defaults.

```javascript
const defaultOptions = {
  //producer defaults
  defaultTopic: 'someTopicName',
  requiredAcks: -1
  timeout: 30000
  partitioner: new Kafka.DefaultPartitioner(),
  retries: {
    attempts: 7, // 2.8 sec @ 7 retries min 100, max 1000 - 100+200+300+400+500+600+700=2800
    delay: {
      min: 100,
      max: 1000,
    },
  },
  batch: {
    size: 16384,
    maxWait: 100
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
    directory: 'kafkaFallbackLogs', // non-ephemeral filesystem mount, shared by all nodes
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
