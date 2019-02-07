import RdKafka from 'node-rdkafka'
import lodash from 'lodash'
// import promiseRetry from 'promise-retry'
import uuidv4 from 'uuid/v4'
import stringify from 'json-stringify-safe'

import { init as initLogger } from './logging'
import { FilePublisher, initLogger as initFilePublisherLogger } from './file-publisher'
import { validateKey } from './validate-key'
import { validateValue } from './validate-value'

// TODO impl reconnect retry logic
// TODO impl consec error count and threshold till fallback and initiating reconnect
// TODO impl fallback clearing ALL pending messages in order (sort by number)
// TODO impl consec error count/clearing on ready, need to clear pending timeouts or it will potentially loop reconnecting
// TODO impl ack promise/reject

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

  // consecutive error threshold till reconnect is initiated
  // cleared on successful delivery report (kafka ack) and on transition to kafka ready
  consecutiveErrorCntReconnectThreshold: 10,

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

// Not practical to mock constructors, workaround - wrap constructors in functions which can be stubbed/mocked
export const kafka = {
  Producer: RdKafka.Producer, // constructor

  // call this to execute constructor, this function can be stubbed/mocked
  initKafkaProducer(options) {
    return new this.Producer(options)
  },
}

export const fallback = {
  Publisher: FilePublisher, // constructor

  // call this to execute constructor, this function can be stubbed/mocked
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
        consecutiveErrorCnt: 0, // consecutive errors since last transition to kafka ready, upon threshold, reconnect
        backgroundRetries: 0, // how many times kafka background recovery probe retries
        lastErrorTs: undefined, // 'ISO8601'
        lastError: undefined, // error message
        lastReset: new Date().toUTCString(), // startup time
        filePublisher: this.fallback.getStatistics(),
      }
      log.info(`ID ${this.id} options ${stringify(this.options)}`)
    } catch (err) {
      // stderr.write because logger not yet initialized
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
      // stdout.write because logger not yet initialized
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
      // TODO what if ready never comes, need a timeout, error logging, and attempting disconnect/reconnect
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

  // eslint-disable-next-line no-unused-vars
  readyEvent(producerInfo, metadata) {
    this.kafkaReady = true
    log.info('ready')
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

  // TODO deprecated, migrate to shutdown
  async end() {
    process.stdout.write('WARN kafka-publisher end() deprecated, switch to shutdown()\n')
    return this.shutdown()
  }

  async shutdown() {
    this.kafkaReady = false
    log.info('shutting down')
    // TODO stop the connect/monitor/recovery process
    return this.disconnect()
  }

  /**
   * key determines the partitioning and order
   * value = message JSON object
   * topic is optional and uses defaultTopic it not provided
   * if cb != null cb is called when corresponding ack received from kafka
   * if cb == null a promise is returned which is resolved when ack received from kafka or rejected if error or timeout
   */
  queue(key, value, topic = this.options.defaultTopic, cb) {
    return this.queueInternal({ key, value, topic }, cb)
  }

  // Internal methods - not for use by clients
  // if cb != null cb is called when corresponding ack received from kafka
  // if cb == null a promise is returned which is resolved when ack received from kafka or rejected if error or timeout
  queueInternal(message, cb) {
    this.stats.msgCnt += 1
    const topic = message.topic || this.options.defaultTopic
    if (topic == null) {
      const err = new Error(`Error defaultTopic or per message queue topic not specified, discarding message ${stringify(message)}`)
      this.updateErrorStatsInternal(err)
      log.error(err.message)
      if (cb != null) {
        cb(err)
      }
      return Promise.reject(err)
    }

    let key
    try {
      key = validateKey(message.key)
      validateValue(message.value)
    } catch (err) {
      log.error(err, `Error validating message key or value ${err.message}, key:${stringify(message.key)}, ${stringify(message.value)}`)
      this.updateErrorStatsInternal(new Error(`Error validating message key or value ${err.message}`))
      if (cb != null) {
        cb(err)
      }
      return Promise.reject(err)
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
      if (cb != null) {
        cb(err)
      }
      return Promise.reject(err)
    }

    // publish or write to fallback
    try {
      const ackKey = this.stats.msgCnt
      const queuedMsg = this.queueMsgAckTimeout(topic, msgValue, key, ackKey, cb)
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
        this.incrementConsecutiveErrorCnt()
        // TODO write to fallback
        const error = new Error(`message failed to queue, writing to fallback, msg ${this.stats.msgCnt}`)
        if (cb != null) {
          cb(error)
        }
        return queuedMsg.settleAndRemove(error)
      }
      this.stats.queueCnt += 1
      return queuedMsg.promise.promise
    } catch (err) {
      this.incrementConsecutiveErrorCnt()
      const error = new Error(`rdkafka queueing error, writing to fallback, msg ${this.stats.msgCnt}, ${err.message}, ${stringify(err)}`)
      log.error(error.message)

      if (err.code != null && err.code === RdKafka.CODES.ERRORS.ERR_MSG_SIZE_TOO_LARGE) {
        // TODO write to fallback too large file
      } else {
        // TODO write to fallback
      }

      if (cb != null) {
        cb(error)
      }
      return Promise.reject(error)
    }
  }

  pending() {
    // console.log(Object.keys(this.q), Object.keys(this.q).length)
    return Object.keys(this.q).length
  }

  queueMsgAckTimeout(topic, msgValue, key, ackKey, cb) {
    const queuedMsg = {
      ackKey, // unique seq for kafka delivery-report to ack this message
      topic,
      msgValue, // the message to queue to kafka
      promise: {
        promise: null, // the promise for awaiting completion, alternative to cb
        resolve: null, // resolve function for resolving promise on delivery-report ack
        reject: null, // reject function for rejecting promise on error or timeout waiting for delivery-report ack
        settled: false, // promises don't have a state variable, used to avoid race with mesg timeout
      },
      cb, // may be null (unused), promise is alternative
      timoutObj: null, // timeout object that will handle timeout if delivery-report ack not received, cancelled on receive
      settleAndRemove: null, // TODO clear timeout, set settled true, remove from pending, if error, reject promise, else resolve promise
    }

    queuedMsg.promise.promise = new Promise((resolve, reject) => {
      queuedMsg.promise.resolve = resolve
      queuedMsg.promise.reject = reject
    })

    queuedMsg.timoutObj = setTimeout((qdMsg) => {
      try {
        if (qdMsg.settled) return
        this.incrementConsecutiveErrorCnt() // will initiate reconnect upon consecutiveErrorCntReconnectThreshold
        queuedMsg.promise.settled = true
        delete this.q[qdMsg.ackKey]
        const error = new Error(`timeout, write to fallback, msg ${qdMsg.ackKey}`)
        queuedMsg.promise.reject(error)
        // TODO write to fallback
        log.warn(error.message)
      } catch (err) {
        log.error(err, 'Error executing mesg timeout, msg %d', qdMsg.ackKey)
      }
    }, this.options.producer['message.timeout.ms'], queuedMsg)

    queuedMsg.settleAndRemove = (error) => {
      if (queuedMsg.promise.settled) return queuedMsg.promise.promise
      clearTimeout(queuedMsg.timoutObj)
      queuedMsg.promise.settled = true
      delete this.q[queuedMsg.ackKey]
      if (error == null) {
        queuedMsg.promise.resolve()
      } else {
        queuedMsg.promise.reject(error)
      }
      return queuedMsg.promise.promise
    }

    this.q[ackKey] = queuedMsg
    return queuedMsg
  }

  deliveryReport(error, report) {
    try {
      this.stats.ackCnt += 1

      if (error != null) {
        this.incrementConsecutiveErrorCnt()
      // TODO handle error, cancel timeout, log, write to fallback
      // log.info('deliveryReport **************', error, report.opaque, this.pending())
      }

      this.stats.consecutiveErrorCnt = 0

      const queuedMsg = this.q[report.opaque]
      if (!queuedMsg) return
      clearTimeout(queuedMsg.timoutObj)
      // remove from pending
      delete this.q[report.opaque]
      // call callback if it exists
      if (queuedMsg.cb != null) queuedMsg.cb(error, report)
      // TODO if no cb, resolve promise
    } catch (err) {
      // TODO handle error, log, potential memory leak since it might not be cleaning up
      log.error(err, err, report) // TODO FIXME
    }
  }

  incrementConsecutiveErrorCnt() {
    this.stats.consecutiveErrorCnt += 1
    if (this.stats.consecutiveErrorCnt >= this.options.consecutiveErrorCntReconnectThreshold) {
      // TODO set kafkaReady false
      // TODO fallback all pending messages, or let them timeout to fallback
      // TODO start retry reconnect logic
    }
  }

  getStatistics() {
    this.stats.kafkaReady = this.kafkaReady
    this.stats.kafkaPending = this.kafkaPending
    this.stats.backgroundRetrying = this.backgroundRetrying
    const stats = lodash.cloneDeep(this.stats)
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
    this.stats.errorCnt += 1
    this.stats.lastError = err.message
    this.stats.lastErrorTs = new Date().toUTCString()
  }
}
