import RdKafka from 'node-rdkafka'
import lodash from 'lodash'
// import promiseRetry from 'promise-retry'
import uuidv4 from 'uuid/v4'
import stringify from 'json-stringify-safe'

import { init as initLogger } from './logging'
import { FilePublisher, initLogger as initFilePublisherLogger } from './file-publisher'
import { validateKey } from './validate-key'
import { validateValue } from './validate-value'

// console.log(RdKafka.CODES.ERRORS.ERR_MSG_SIZE_TOO_LARGE)

let log

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
    'message.timeout.ms': 10000, // default 300000 (5 min) - deliviery timeout, ideally (request.timeout.ms x message.send.max.retries)
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

  // fallback defaults - where to write to filesystem
  fallback: {
    // directory: 'kafkaFallbackLogs', // non-ephemeral filesystem mount, shared by all nodes
    // // recovery ownershipTimeoutMs: 60000, // timeout to lose ownership
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

  // pino logging options
  logging: {
    name: 'kafka-publisher',
    // enabled: true,
    // level: info,
    // timestamp: true,
    prettyPrint: process.env.NODE_ENV === 'DEBUG' || false,
    useLevelLabels: true,
  },
}

// Not possible to mock constructors usefully, workaround - wrap constructors in functions which can be stubbed/mocked
export const kafka = {
  Producer: RdKafka.Producer, // constructor

  // call this which calls constructor, this function can be stubbed/mocked
  initKafkaProducer(options) {
    return new this.Producer(options)
  },
}

export const fallback = {
  Publisher: FilePublisher,

  initFallbackPublisher(filePublisherOptions) {
    return new this.Publisher(filePublisherOptions)
  },
}

export class KafkaPublisher {
  constructor(options) {
    try {
      this.validateOptions(options)

      this.q = {}
      this.id = uuidv4()
      this.kafkaPending = false // true if kafka trying to connect or send pending completion, i.e., timeout, but still pending promise
      this.kafkaReady = false // false if kafka last had error and is not ready to send
      this.backgroundRetrying = false // true if actively/periodically backgroundRetrying polling for kafka to become available
      this.publishing = false
      this.options = lodash.merge(defaultOptions, options)
      log = initLogger(this.options.logging)
      initFilePublisherLogger()
      this.kafkaProducer = kafka.initKafkaProducer(this.options.producer)
      const filePublisherOptions = this.options.fallback
      filePublisherOptions.id = this.id
      this.fallback = fallback.initFallbackPublisher(filePublisherOptions)
      this.stats = {
        msgCnt: 0, // total message queue calls since start/reset, queue errors (not ready or connected) are counted
        queueCnt: 0, // total messages queued since start/reset, queue errors (not ready or connected) are NOT counted
        ackCnt: 0, // messages successfully sent and ack'd by kafka, possible for ack's to be lost on client reconnects or broker fail-over
        errorCnt: 0, // total errors since start/reset, includes all retries
        backgroundRetries: 0, // how many times kafka background recovery probe retries
        lastErrorTs: undefined, // 'ISO8601'
        lastError: undefined, // error message
        lastReset: new Date().toUTCString(), // startup time
        filePublisher: this.fallback.getStatistics(),
      }
      log.info(`KafkaPublisher ${this.id} options ${stringify(this.options)}`)
    } catch (err) {
      process.stderr.write(`ERROR initializing KafkaPublisher options ${stringify(this.options)} ${stringify(err)}\n`)
      throw err
    }
  }

  // eslint-disable-next-line class-methods-use-this
  validateOptions(options) {
    if (options == null) {
      throw new Error('KafkaPublisher options must not be null')
    }
    // allow migration from options.connectionString to options.producer.metadata.broker.list
    if (options.connectionString != null) {
      process.stdout.write('WARN kafka-publisher connectionString deprecated, switch to producer."metadata.broker.list"\n')
      if (options.producer == null) {
        // eslint-disable-next-line no-param-reassign
        options.producer = {}
      }
      // eslint-disable-next-line no-param-reassign
      options.producer['metadata.broker.list'] = options.connectionString
    }
    if (options.producer['metadata.broker.list'] == null) {
      throw new Error('KafkaPublisher producer."metadata.broker.list" must be set with kafka seed broker list, i.e., 127.0.0.1:9092,...')
    }
  }

  // if configuration is not correct, this will retry forever logging error on each retry
  // Only await if desire is to await initial kafka connection
  // if client call uses await or .then, it prevents fallback on startup - NOT desirable when leveraging fallback behavior
  async init() {
    // await promiseRetry(async (retry) => {
    try {
      this.kafkaProducer.setPollInterval(this.options.producerPollIntervalMs)

      this.kafkaProducer
        .on('disconnected', producerInfo => this.disconnectEvent(producerInfo))
        .on('ready', (producerInfo, metadata) => this.readyEvent(producerInfo, metadata))
        .on('event.error', err => this.errorEvent(err))
        .on('event.throttle', info => log.warn('event.throttle %o', info))
        .on('delivery-report', (err, report) => this.deliveryReport(err, report))

      // TODO start the connect/monitor/recovery process
      // TODO what if this never comes, need a timeout, error logging, and attempting recovery
      this.kafkaProducer.connect() // new node-rdkafka.Producer().connect()
    } catch (err) {
      log.error(err, 'Error initializing kafka client, retrying')
      this.kafkaProducer.disconnect()
      this.updateErrorStatsInternal(err)
        // retry(err)
    }
    // }, this.options.retryOptions)
  }

  errorEvent(err) {
    this.kafkaReady = false
    log.error(err, 'event.error')
  }

  readyEvent(producerInfo, metadata) {
    this.kafkaReady = true
    log.info('ready %o')
  }

  disconnectEvent(producerInfo) {
    this.kafkaReady = false
    log.info('disconnected %o', producerInfo)
  }

  poll() {
    this.kafkaProducer.poll()
  }

  connect() {
    this.kafkaProducer.connect()
  }

  disconnect() {
    log.info('disconnecting')
    return this.kafkaProducer.disconnect()
  }

  async end() {
    this.kafkaReady = false
    log.info('shutting down')
    // TODO stop the connect/monitor/recovery process
    return this.disconnect()
  }

  /**
   * key determines the partitioning and order
   * value = message JSON object
   * topic is optional and uses defaultTopic it not provided
   * cb is called when corresponding ack received from kafka
   */
  queue(key, value, topic = this.options.defaultTopic, cb) {
    return this.queueInternal({ key, value, topic }, cb)
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
  }

  // Internal methods - not for use by clients
  // cb is called when corresponding ack received from kafka
  queueInternal(message, cb) {
    // eslint-disable-next-line no-plusplus
    ++this.stats.msgCnt
    const topic = message.topic || this.options.defaultTopic
    if (topic == null) {
      const err = new Error(`Error defaultTopic or per message queue topic not specified, discarding message ${stringify(message)}`)
      this.updateErrorStatsInternal(err)
      log.error(err.message)
      return
    }

    let key
    try {
      key = validateKey(message.key)
      validateValue(message.value)
    } catch (err) {
      log.error(err, `Error validating message key or value ${err.message}, key:${stringify(message.key)}, ${stringify(message.value)}`)
      this.updateErrorStatsInternal(new Error(`Error validating message key or value ${err.message}`))
      return
    }

    let msgValue
    if (message.value != null) {
      msgValue = stringify(message.value)
    } else if (message.message != null) { // in case client uses message instead of value
      msgValue = stringify(message.message)
    } else {
      const err = new Error(`Error message did not contain message or value property, discarding message ${stringify(message)}`)
      this.updateErrorStatsInternal(err)
      log.error(err.message)
    }

// TODO publish or write to fallback
    try {
      const ackKey = this.stats.msgCnt
      this.queueMsgAckTimeout(topic, msgValue, key, ackKey, cb)
      // produce returns true | false if queued
      const queued = this.kafkaProducer.produce(
        topic,
        null,
        Buffer.from(msgValue),
        key,
        Date.now(),
        ackKey,
      )
      if (!queued) {
        // TODO clear ack timeout promise
        // TODO what to do if returns false - write to fallback and start recovery
        log.debug('NOT queued, write to fallback', this.stats.msgCnt)
      } else {
        // eslint-disable-next-line no-plusplus
        ++this.stats.queueCnt
      }
    } catch (err) {
      // TODO clear ack timeout promise
      // TODO meaningfully handle error
      log.error(err, 'produce error %d', this.stats.msgCnt)
    }
  }

  pending() {
    // console.log(Object.keys(this.q), Object.keys(this.q).length)
    return Object.keys(this.q).length
  }

  queueMsgAckTimeout(topic, msgValue, key, ackKey, cb) {
    const queuedMsg = {
      ackKey,
      topic,
      msgValue,
      resolved: false,
      cb,
    }
    queuedMsg.timoutObj = setTimeout((qdMsg) => {
      if (qdMsg.resolved) return
      delete this.q[qdMsg.ackKey]
      // TODO remove
      log.debug('timeout', qdMsg.ackKey)
    }, this.options.producer['message.timeout.ms'], queuedMsg)
    this.q[ackKey] = queuedMsg
  }

  deliveryReport(error, report) {
    // eslint-disable-next-line no-plusplus
    ++this.stats.ackCnt

    // TODO remove
    if (error != null) log.debug('deliveryReport', error, report.opaque, this.pending())

    try {
      const queuedMsg = this.q[report.opaque]
      if (!queuedMsg) return
      clearTimeout(queuedMsg.timoutObj)
      // remove from pending
      delete this.q[report.opaque]
      // call callback if it exists
      if (queuedMsg.cb != null) queuedMsg.cb(error, report)
      // TODO if no cb, resolve promise
    } catch (err) {
      // TODO
      log.debug(err, err, report)
    }
  }

  getStatistics() {
    this.stats.kafkaReady = this.kafkaReady
    this.stats.kafkaPending = this.kafkaPending
    this.stats.backgroundRetrying = this.backgroundRetrying
    const stats = lodash.cloneDeep(this.stats)
    delete stats.connectionString
    return stats
  }

  resetStatistics() {
    this.stats.ackCnt = 0 // messages successfully sent to kafka
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
