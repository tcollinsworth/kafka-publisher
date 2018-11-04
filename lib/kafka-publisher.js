import NoKafka from 'no-kafka'
import promiseRetry from 'promise-retry'
import uuidv4 from 'uuid/v4'
import stringify from 'json-stringify-safe'
import { timeout, TimeoutError } from 'promise-timeout'

import { getLogger } from './logging'
import { FilePublisher } from './file-publisher'

const log = {
  error: getLogger('kafka-publisher:error'),
  warn: getLogger('kafka-publisher:warn'),
  info: getLogger('kafka-publisher:info'),
  debug: getLogger('kafka-publisher:debug'),
}

const defaultOptions = {
  // producer
  defaultTopic: undefined,
  requiredAcks: -1, // default 1
  // default timeout: 30000,
  // default partitioner: new Kafka.DefaultPartitioner(),
  retries: {
    attempts: 7, // 2.8 sec @ 7 retries min 100, max 1000 - 100+200+300+400+500+600+700=2800
    delay: {
      min: 100,
      max: 1000, // default 3000
    },
  },
  batch: {
    size: 16384,
    maxWait: 100, // default 10
  },
  // default codec: Kafka.COMPRESSION_NONE,

  // client
  // default clientId: 'no-kafka-client',
  connectionString: 'FIXME', // default process.env.KAFKA_URL || 'kafka://127.0.0.1:9092',
  // default ssl: {
  //   cert: process.env.KAFKA_CLIENT_CERT,
  //   key: process.env.KAFKA_CLIENT_CERT_KEY,
  //   // secureProtocol: 'TLSv1_method',
  //   rejectUnauthorized: false,
  //   // ciphers: 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA',
  //   ca: process.env.KAFKA_CLIENT_CA
  // },
  // default asyncCompression: true,
  // default brokerRedirection: false,
  reconnectionDelay: {
    min: 1000,
    max: 10000, // default 1000
  },
  // default logger: {
  //   logLevel: 5,
  // }

  // init retry options
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
    // directory: 'kafkaFallbackLogs', // non-ephemeral filesystem mount, shared by all nodes
    // // TODO recovery ownershipTimeoutMs: 60000, // timeout to lose ownership
    // retryOptions: {
    //   retries: 5, // not strictly required, however disables creating default retry table
    //   // retries: 10000, // 10K ~2 months - creates a retry schedule for all retries (rediculous, why not computing) 8 9's causes FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory
    //   // forever: true, // use this instead of retries or it will create a lookup table for all retries wasting cycles and memory
    //   factor: 2,
    //   minTimeout: 100, // 0.1 sec
    //   maxTimeout: 2000, // 2 sec
    //   randomize: true,
    // },
  },
}

// Couldn't get sinon constructor stub to work, this allows setting mocks for constructors
export const kafka = {
  Producer: NoKafka.Producer, // constructor

  // call these which call above replace with mocks
  initKafkaProducer(options) {
    return new NoKafka.Producer(options)
  },
}

export class KafkaPublisher {
  constructor(options) {
    if (options == null || options.connectionString == null) {
      throw new Error('KafkaPublisher options.connectionString must be set with kafka seed broker list, i.e., 127.0.0.1:9092,...')
    }

    this.id = uuidv4()
    this.kafkaPending = false // true if kafka trying to connect or send pending completion, i.e., timeout, but still pending promise
    this.kafkaReady = false // false if kafka last had error and is not ready to send
    this.backgroundRetrying = false // true if actively periodically backgroundRetrying polling for kafka to become available
    this.q = []
    this.options = Object.assign(defaultOptions, options)
    this.kafkaProducer = kafka.initKafkaProducer(this.options)
    const filePublisherOptions = this.options.fallback
    filePublisherOptions.id = this.id
    this.fallback = new FilePublisher(filePublisherOptions)
    this.stats = {
      queueCnt: 0, // total messages queued since start/reset
      curQueuedCnt: 0, // how many currently in queue
      sentCnt: 0, // messages successfully sent to kafka
      errorCnt: 0, // total errors since start/reset, includes all retries
      backgroundRetries: 0, // how many times kafka background recovery probe retries
      lastErrorTs: undefined, // 'ISO8601'
      lastError: undefined, // error message
      lastReset: new Date().toUTCString(), // startup time
      filePublisher: this.fallback.getStatistics(),
    }
    log.info(`KafkaPublisher ${this.id} options ${stringify(this.options)}`)
  }

  // if configuration is not correct, this will retry forever logging error on each retry
  // Only await if desire is to await initial kafka connection
  // if client call uses await or .then, it prevents fallback on startup - NOT desirable when leveraging fallback behavior
  async init() {
    await promiseRetry(async (retry) => {
      try {
        await this.kafkaProducer.init()
        log.debug('Successfully initialized kafka client')
        this.kafkaReady = true
      } catch (err) {
        this.updateErrorStatsInternal(err)
        log.error('Error initializing kafka client', err)
        retry(err)
      }
    }, this.retryOptions)
  }

  async end() {
    return this.kafkaProducer.end()
  }

  /**
   * key determines the partitioning and order
   * value = message JSON object
   * topic is optional and uses defaultTopic it not provided
   */
  queue(key, value, topic = this.options.defaultTopic) {
    this.queueInternal({ key, value, topic })
    setImmediate(this.handleQueuedInternal.bind(this))
  }

  /**
   * key determines the partitioning and order
   * value = message JSON object
   * topic is optional and uses defaultTopic it not provided
   * const messageArray = [
   *   { key, value|message [, topic] },
   *   { key, value|message [, topic] },
   *   ...
   * ]
   */
  queueMessages(messageArray) {
    messageArray.forEach((message) => {
      this.queueInternal(message)
    })
    setImmediate(this.handleQueuedInternal.bind(this))
  }

  // Internal methods

  queueInternal(message) {
    const topic = message.topic || this.options.defaultTopic
    if (topic == null) {
      const err = new Error(`Error defaultTopic or per message queue topic not specified, discarding message ${stringify(message)}`)
      this.updateErrorStatsInternal(err)
      log.error(err.message)
      return
    }

    let value
    if (message.value != null) {
      value = message.value
    } else if (message.message != null) {
      value = message.message
    } else {
      const err = new Error(`Error message did not contain message or value property, discarding message ${stringify(message)}`)
      this.updateErrorStatsInternal(err)
      log.error(err.message)
    }

    this.q.push({
      topic,
      key: message.key,
      value,
    })

    // eslint-disable-next-line no-plusplus
    ++this.stats.queueCnt
  }

  async handleQueuedInternal() {
    if (this.publishing || this.q.length < 1) return

    try {
      this.publishing = true // only want one loop/continuation handling/mutating/publishing from queue
      while (this.q.length > 0) {
        const mesg = this.q[0] // peek first element from array
        if (this.kafkaReady) {
          // eslint-disable-next-line no-await-in-loop
          if (await this.publishInternal(mesg.topic, mesg.key, mesg.value)) {
            // eslint-disable-next-line no-plusplus
            ++this.stats.sentCnt
            this.q.shift() // remove first element from array
            // eslint-disable-next-line no-continue
            continue
          }
        }
        // eslint-disable-next-line no-await-in-loop
        await this.fallback.send(mesg) // best effort with retries, may cause dupes, consuming must be idempotent
        this.q.shift() // remove first element from array
      }
    } catch (err) {
      log.error(`Error handling queued ${err.message}`, err)
    } finally {
      this.publishing = false
    }
  }

  // return true if successfully published to kafka
  // return false to fallback
  async publishInternal(topic, key, value) {
    const kafkaMesg = { topic, message: { key: stringify(key), value: stringify(value) } }
    try {
      try {
        validateKey(key)
        validateValue(value)
      } catch (err) {
        log.error(`Error validating message key or value ${err.message}, key:${stringify(key)}, ${stringify(value)}`, err)
        this.updateErrorStatsInternal(new Error(`Error validating message key or value ${err.message}`))
        return true
      }

      // If already a pending kafka operation, DO NOT start another
      if (this.kafkaPending) return false

      // completes or it times out continuing to try to connect to kafka in the background
      // result depends on send success, timeout throws error which causes this.kafkaReady = false
      this.kafkaReady = await timeout(this.kafkaSendBlocksWhileDownInternal(kafkaMesg), 3000)
      return this.kafkaReady
    } catch (err) {
      if (err instanceof TimeoutError) {
        this.kafkaPending = true
        this.kafkaReady = false
      }

      log.error(`Error publishing to kafka ${err.message}`)
      this.updateErrorStatsInternal(new Error(`Error publishing to kafka ${err.message}`))
      return false
    }
  }

  async kafkaSendBlocksWhileDownInternal(kafkaMesg) {
    try {
      // send will not return while kafka unavailable, it continually retries to connect
      const resp = await this.kafkaProducer.send(kafkaMesg)

      log.debug(stringify(resp), stringify(kafkaMesg))

      if (resp == null || resp.length < 1 || resp[0].error != null) {
        // if message too large error, log and skip, do NOT set kafkaReady false
        if (resp != null && resp.length > 0 && resp[0].error != null && resp[0].error.code === 'MessageSizeTooLarge') {
          log.error(`Kafka MessageSizeTooLarge (default > 1 MB), discarding ${stringify(kafkaMesg)}`)
          this.updateErrorStatsInternal(new Error('Kafka MessageSizeTooLarge (default > 1 MB), discarding'))
          return true
        }

        log.error(`Error publishing to kafka ${stringify(resp)}, ${stringify(kafkaMesg)}`)
        this.updateErrorStatsInternal(new Error(`Error publishing to kafka ${resp[0].error.code}`))
        this.kafkaReady = false
        // start background process to keep retrying until kafka recovers
        if (!this.backgroundRetrying) setImmediate(this.kafkaBackroundRetryInternal.bind(this), kafkaMesg.topic)
        return false
      }
      return true
    } catch (err) {
      log.error(`Error publishing to kafka ${err.message}, ${stringify(kafkaMesg)}`, err)
      this.updateErrorStatsInternal(new Error(`Error publishing to kafka ${err.message}`))
      return false
    } finally {
      this.kafkaPending = false
    }
  }

  async kafkaBackroundRetryInternal(topic) {
    this.backgroundRetrying = true
    // eslint-disable-next-line no-plusplus
    ++this.stats.backgroundRetries
    try {
      this.kafkaReady = await this.publishInternal(topic, 'test', { test: true })
      if (this.kafkaReady) {
        this.backgroundRetrying = false
        log.info('Kafka recovered, switching from file append fallback, manually load fallback messages and delete file')
      }
    } catch (err) {
      log.error(`Error kafkaBackroundRetries topic:${topic} ${err.message}`, err)
      this.updateErrorStatsInternal(new Error(`Error kafkaBackroundRetries topi:${topic} ${err.message}`))
    } finally {
      // retry ever 10 seconds, logging every time
      if (!this.kafkaPending && !this.kafkaReady) setTimeout(this.kafkaBackroundRetryInternal.bind(this), 10000, topic)
    }
  }

  queued() {
    return this.q.length
  }

  getStatistics() {
    this.stats.curQueuedCnt = this.q.length
    this.stats.kafkaReady = this.kafkaReady
    this.stats.kafkaPending = this.kafkaPending
    this.stats.backgroundRetrying = this.backgroundRetrying
    return this.stats
  }

  resetStatistics() {
    this.stats.queueCnt = 0 // total messages queued since start/reset
    this.stats.curQueuedCnt = 0 // how many currently in queue
    this.stats.sentCnt = 0 // messages successfully sent to kafka
    this.stats.errorCnt = 0 // total errors since start/reset, includes all retries
    this.stats.backgroundRetries = 0 // how many times kafka background recovery probe retries
    this.stats.lastErrorTs = undefined // 'ISO8601'
    this.stats.lastError = undefined // error message
    this.stats.lastReset = new Date().toUTCString() // startup time
    this.stats.filePublisher.resetStatistics()
  }

  updateErrorStatsInternal(err) {
    // eslint-disable-next-line no-plusplus
    ++this.stats.errorCnt
    this.stats.lastError = err.message
    this.stats.lastErrorTs = new Date().toUTCString()
  }
}

// discard message if bad key - (null, undefined, Boolean, Symbol) must be (string, number, object)
function validateKey(key) {
  if (key == null || typeof key === 'boolean' || typeof key === 'symbol') {
    throw new Error(`key should be a number, string, or object, was ${typeof key}`)
  }
}

// discard message if bad value - not JSON object
function validateValue(value) {
  if (value == null || typeof value !== 'object') {
    throw new Error(`value should be an object, was ${typeof value}`)
  }
}
