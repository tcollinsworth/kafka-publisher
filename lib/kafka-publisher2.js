import RdKafka from 'node-rdkafka'
import lodash from 'lodash'
import promiseRetry from 'promise-retry'
import uuidv4 from 'uuid/v4'
import stringify from 'json-stringify-safe'
import debug from 'debug'

import { getLogger } from './logging'
import { FilePublisher } from './file-publisher'
import { validateKey } from './validate-key'
import { validateValue } from './validate-value'

// console.log(RdKafka.CODES.ERRORS.ERR_MSG_SIZE_TOO_LARGE)

const log = {
  error: getLogger('error'),
  warn: getLogger('warn'),
  info: getLogger('info'),
  debug: getLogger('debug'),
}

const defaultOptions = {
  // deprecated, legacy, from no-kafka config
  connectionString: 'FIXME', // default process.env.KAFKA_URL || '27.0.0.1:9092',

  defaultTopic: undefined,

  producer: {
    'client.id': 'kafka-publisher', // default rdkafka
    'metadata.broker.list': '127.0.0.1:9092', // default none
    // request.required.acks: 1, // default 1, -1 all ISR
    // request.timeout.ms: 5000, // default 5 sec
    'message.timeout.ms': 10000, // default 300000 (5 min) - deliviery timeout
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
    // debug: 'all', // TODO disable
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

// Not possible to mock constructors usefully, workaround - wrap constructors in functions which can be stubbed/mocked
export const kafka = {
  Producer: RdKafka.Producer, // constructor

  // call this which calls constructor, this function can be stubbed/mocked
  initKafkaProducer(options) {
    console.log(options)
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
    // TODO allow migration from options.connectionString to options.producer.metadata.broker.list
    // TODO if options.connectionString, copy to options.producer.metadata.broker.list, but warn to migrate
    // TODO warn and use default if no options.producer.metadata.broker.list
    // TODO warn if no clientId set, but use default
    if (options == null || options.connectionString == null) {
      throw new Error('KafkaPublisher options.producer.metadata.broker.list must be set with kafka seed broker list, i.e., 127.0.0.1:9092,...')
    }

    this.cnt = 0 // TODO remove
    this.q = {}
    this.id = uuidv4()
    this.kafkaPending = false // true if kafka trying to connect or send pending completion, i.e., timeout, but still pending promise
    this.kafkaReady = false // false if kafka last had error and is not ready to send
    this.backgroundRetrying = false // true if actively/periodically backgroundRetrying polling for kafka to become available
    this.publishing = false
    this.options = Object.assign(defaultOptions, options)
    this.kafkaProducer = kafka.initKafkaProducer(this.options.producer)
    const filePublisherOptions = this.options.fallback
    filePublisherOptions.id = this.id
    this.fallback = fallback.initFallbackPublisher(filePublisherOptions)
    this.stats = {
      msgCnt: 0, // total message queue calls since start/reset, queue errors (not ready or connected) are counted
      queueCnt: 0, // total messages queued since start/reset, queue errors (not ready or connected) are NOT counted
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
    // await promiseRetry(async (retry) => {
    try {
      this.kafkaProducer.setPollInterval(100)

      this.kafkaProducer
        // .on('disconnected', (err, m) => console.log('disconnected_', err, m))
        .on('ready', (err, m) => console.log('ready', err, m))
        // .on('event', (m) => console.log('event_', m))
        // .on('event.log', (m) => console.log('event.log_', m))
        // .on('event.stats', (m) => console.log('event.stats_', m))
        .on('event.error', err => console.log('event.error', err))
        .on('event.throttle', (err, m) => console.log('event.throttle_', err, m))
        .on('delivery-report', (err, report) => this.deliveryReport(err, report))
        // , err, report)) // received when successfully delivered

        // .on('rebalance', (err, m) => console.log('rebalance_', err, m))
        // .on('socket', (err, m) => console.log('socket_', err, m))
        // .on('event.socket', (err, m) => console.log('event.socket_', err, m))
        // .on('closesocket', (err, m) => console.log('closesocket_', err, m))
        // .on('event.closesocket', (err, m) => console.log('event.closesocket_', err, m))
        // .on('connect', (err, m) => console.log('connect_', m))
        // .on('event.connect', (err, m) => console.log('event.connect_', err, m))
        // .on('open', (err, m) => console.log('open_', err, m))
        // .on('event.open', (err, m) => console.log('event.open_', err, m))

      this.kafkaProducer.connect() // new node-rdkafka.Producer().connect()

      this.kafkaProducer.on('ready', () => {
        this.kafkaReady = true
        log.debug('Successfully initialized kafka client')
      })
        // TODO what if this never comes, need a timeout watching for this and attempting recovery
    } catch (err) {
      log.error('Error initializing kafka client, retrying', err)
      this.kafkaProducer.disconnect()
      this.updateErrorStatsInternal(err)
        // retry(err)
    }
    // }, this.options.retryOptions)
  }

  poll() {
    this.kafkaProducer.poll()
  }

  connect() {
    this.kafkaProducer.connect()
  }

  async end() {
    // TODO stop the monitor/recovery process
    return this.kafkaProducer.disconnect()
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
    ++this.cnt
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
      log.error(`Error validating message key or value ${err.message}, key:${stringify(message.key)}, ${stringify(message.value)}`, err)
      this.updateErrorStatsInternal(new Error(`Error validating message key or value ${err.message}`))
      return
    }

    let value
    if (message.value != null) {
      value = message.value
    } else if (message.message != null) { // in case client uses message instead of value
      value = message.message
    } else {
      const err = new Error(`Error message did not contain message or value property, discarding message ${stringify(message)}`)
      this.updateErrorStatsInternal(err)
      log.error(err.message)
    }

// TODO publish or write to fallback
    try {
      const msgValue = stringify(message.value)
      const ackKey = ++this.stats.msgCnt
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
        console.log('NOT queued, write to fallback', this.cnt)
      } else {
        // eslint-disable-next-line no-plusplus
        ++this.stats.queueCnt
      }
    } catch (err) {
      // TODO clear ack timeout promise
      // TODO
      console.log('produce error', this.cnt, err)
    }
  }

  pending() {
    //console.log(Object.keys(this.q), Object.keys(this.q).length)
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
      console.log('timeout', qdMsg.ackKey)
    }, 10000, queuedMsg)
    this.q[ackKey] = queuedMsg
  }

  deliveryReport(error, report) {
    if (report.opaque % 10000 == 0) {
      console.log('deliveryReport', error, report.opaque, this.pending())
    }
    try {
      const queuedMsg = this.q[report.opaque]
      if (!queuedMsg) return
      //console.log('clearing timeout')
      clearTimeout(queuedMsg.timoutObj)
      delete this.q[report.opaque]
      queuedMsg.cb(error, report)
    } catch (err) {
      // TODO
      console.log(err, err, report)
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
