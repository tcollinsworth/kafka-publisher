import RdKafka from 'node-rdkafka'
import lodash from 'lodash'
import promiseRetry from 'promise-retry'
import uuidv4 from 'uuid/v4'
import stringify from 'json-stringify-safe'
import delay from 'delay'

import { init as initLogger } from './logging'
import { FilePublisher, initLogger as initFilePublisherLogger } from './file-publisher'
import { validateKey } from './validate-key'
import { validateValue } from './validate-value'

// TODO impl consec error count and threshold till fallback and initiating reconnect
// TODO impl fallback clearing ALL pending messages in order (sort by number)
// TODO impl consec error count/clearing on ready, need to clear pending timeouts or it will potentially loop reconnecting
// TODO impl ack promise/reject
// TODO validate all mesg error stats updating
// TODO validate try/catch everywhere to avoid uncaught exceptions, especially in catch clauses

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
  consecutiveKafkaErrorCntReconnectThreshold: 10,
  kafkaReadyOrErrorOrTimeoutMs: 60000, // 1 min
  kafkaReadyOrErrorOrTimeoutPollMs: 100, // 100 ms
  kafkaReconnectRetryMaxJitterMs: 10000, // 0-10 sec

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
    level: 'debug', // default 'info' //TODO comment out or set to 'info'
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

      this.initExecuting = false // used to prevent concurrent init executions
      this.connExecuting = false // used to prevent concurrent connect executions
      this.kafkaReady = false // false if kafka last had error and is not ready to send
      this.kafkaEventErrorCnt = 0 // total kafka event errors, used by startKafkaConnectRetryProcess for error detection
      this.lastKafkaEventError = null // used by startKafkaConnectRetryProcess for reporting retry error
      this.consecutiveKafkaErrorCnt = 0 // consecutive kafka errors since last transition to kafka ready, when exceeds threshold, reconnect

      this.options = lodash.merge(defaultOptions, options)
      log = initLogger(this.options.logging)
      log.info(`ID ${this.id} options ${stringify(this.options)}`)

      this.kafkaProducer = kafka.initKafkaProducer(this.options.producer)

      initFilePublisherLogger()
      const filePublisherOptions = this.options.fallback
      filePublisherOptions.id = this.id
      this.fallback = fallback.initFallbackPublisher(filePublisherOptions)

      this.stats = {
        msgCnt: 0, // total message queue calls since start/reset, queue errors (not ready or connected) are counted
        queueCnt: 0, // total messages queued since start/reset, queue errors (not ready or connected) are NOT counted
        ackCnt: 0, // messages successfully sent and ack'd by kafka, possible for ack's to be lost on client reconnects or broker fail-over
        errorCnt: 0, // total errors since start/reset, includes all retries
        lastErrorTs: undefined, // 'ISO8601'
        lastError: undefined, // error message
        lastStatsReset: new Date().toUTCString(), // startup time
        filePublisher: this.fallback.getStatistics(),
      }
    } catch (err) {
      // uses process.stderr.write because logger not initialized due to error with options
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
      // uses process.stdout.write because logger not yet initialized, uses the options this is validating
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

  // if configuration is not correct, this will retry forever logging errors on each retry
  // Only await if desire is to await initial kafka connection
  // if client call uses await or .then, it prevents fallback on startup - which is NOT desirable when leveraging fallback behavior
  async init() {
    log.info('init')
    if (this.initExecuting) {
      log.debug('startKafkaConnectRetryProcess already executing')
      return Promise.reject(new Error('Init currently executing'))
    }
    try {
      this.initExecuting = true
      this.kafkaProducer.setPollInterval(this.options.producerPollIntervalMs)

      this.kafkaProducer
        .on('disconnected', producerInfo => this.disconnectEvent(producerInfo))
        .on('ready', (producerInfo, metadata) => this.readyEvent(producerInfo, metadata))
        .on('event.error', err => this.errorEvent(err))
        .on('event.throttle', info => log.warn('event.throttle %o', info))
        .on('delivery-report', (err, report) => this.deliveryReport(err, report))

      return this.startKafkaConnectRetryProcess()
    } catch (err) {
      const error = new Error(`Error initializing kafka client, shutting down ${err.message}`)
      log.fatal(err, error.message)
      setTimeout(() => process.exit(1), 2000) // give it time to log
      return Promise.reject(error)
    } finally {
      this.initExecuting = false
    }
  }

  async startKafkaConnectRetryProcess() {
    await promiseRetry(async (retry) => {
      if (this.connExecuting) {
        log.debug('startKafkaConnectRetryProcess already executing')
        return
      }
      try {
        this.connExecuting = true
        log.info('startKafkaConnectRetryProcess')
        this.disconnect()
        this.kafkaProducer.connect()
        await this.kafkaReadyOrErrorOrTimeout()
      } catch (err) {
        const error = new Error(`Error trying to connect to kafka, retrying ${err.message}`)
        log.error(err, error.message)
        this.disconnect()
        this.updateErrorStatsInternal(err)
        log.debug('startKafkaConnectRetryProcess calling retry(err)')
        // avoid all nodes retrying reconnect at the same time
        await delay(Math.floor(Math.random() * Math.floor(this.kafkaReconnectRetryMaxJitterMs)))
        retry(err)
      } finally {
        log.debug('startKafkaConnectRetryProcess clearing connExecuting')
        this.connExecuting = false
      }
    }, this.options.retryOptions)
  }

  async kafkaReadyOrErrorOrTimeout() {
    const timeoutTimeMs = Date.now() + this.options.kafkaReadyOrErrorOrTimeoutMs
    const initialKafkaEventErrorCnt = this.kafkaEventErrorCnt

    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (Date.now() >= timeoutTimeMs) {
        throw new Error('timeout waiting for kafka ready or error while connecting')
      }

      if (this.kafkaReady) {
        return
      }

      if (this.kafkaEventErrorCnt !== initialKafkaEventErrorCnt) {
        throw new Error(`error while trying to reconnect ${stringify(this.lastKafkaEventError)}`)
      }

      // eslint-disable-next-line no-await-in-loop
      await delay(this.kafkaReadyOrErrorOrTimeoutPollMs)
    }
  }

// TODO should this kafkaReady=false immediately
// TODO should it start the recovery logic or a timeout to start the recovery logic if not kafkaReady before timeout
  errorEvent(err) {
    this.kafkaReady = false
    this.kafkaEventErrorCnt += 1
    this.lastKafkaEventError = err
    log.error(err, 'kafka event.error')
    this.updateErrorStatsInternal(err)
  }

  // eslint-disable-next-line no-unused-vars
  readyEvent(producerInfo, metadata) {
    this.kafkaReady = true
    this.lastKafkaEventError = null
    this.consecutiveKafkaErrorCnt = 0
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

  async shutdown() {
    return this.disconnect()
  }

  async disconnect() {
    try {
      return this.kafkaProducer.disconnect()
    } catch (err) {
      return Promise.reject(err)
    }
  }

  /**
   * key determines the partitioning and order
   * value = message JSON object
   * topic is optional and uses defaultTopic it not provided
   * promise is returned which is resolved when ack received from kafka or rejected if error or timeout
   *
   * promise resolve(deliveryReport)
   * promise reject(error)
   * <pre>
   * kafka delivery-report example
   * { topic: 'test-topic',
   *   partition: 0,
   *   offset: 537,
   *   key: <--- this is the partitioning key, not the ack message key
   *    <Buffer 37 31 63 34 37 62 38 35 2d 35 64 61 64 2d 34 33 61 62 2d 39 63 37 34 2d 30 36 32 65 33 38 35 31 66 62 30 31>,
   *   opaque: 11, <--- this is the ack message key, not the paritioning key
   *   timestamp: 1549556690566,
   *   size: 868 }
   * </pre>
   */
  async queue(key, value, topic = this.options.defaultTopic) {
    return this.queueInternal({ key, value, topic })
  }

  // Internal methods - not for use by clients
  // promise is returned which is resolved when ack received from kafka or rejected if error or timeout
  async queueInternal(message) {
    this.stats.msgCnt += 1
    const topic = message.topic || this.options.defaultTopic
    if (topic == null) {
      const err = new Error(`Error defaultTopic or per message queue topic not specified, discarding message ${stringify(message)}`)
      this.updateErrorStatsInternal(err)
      log.error(err.message)
      return Promise.reject(err)
    }

    let key
    try {
      key = validateKey(message.key)
      validateValue(message.value)
    } catch (err) {
      log.error(err, `Error validating message key or value ${err.message}, key:${stringify(message.key)}, ${stringify(message.value)}`)
      this.updateErrorStatsInternal(new Error(`Error validating message key or value ${err.message}`))
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
      return Promise.reject(err)
    }

    // publish or write to fallback
    try {
      const ackKey = this.stats.msgCnt
      if (!this.kafkaReady) {
        // TODO write to fallback
        return Promise.reject(new Error(`kafka NOT ready, writing to fallback, msg ${this.stats.msgCnt}`))
      }
      const queuedMsg = this.queueMsgAckTimeout(topic, msgValue, key, ackKey)
      // produce returns true | false if queued
      const queued = this.kafkaProducer.produce(
        topic,
        null, // partition
        Buffer.from(msgValue),
        key, // partition key
        Date.now(),
        ackKey, // ack unique key returned in delivery report
      )
      if (!queued) {
        this.incrementConsecutiveKafkaErrorCnt()
        // TODO write to fallback
        const error = new Error(`message failed to queue, writing to fallback, msg ${this.stats.msgCnt}`)
        return queuedMsg.settleAndRemove(error)
      }
      this.stats.queueCnt += 1
      return queuedMsg.promise.promise
    } catch (err) {
      const error = new Error(`rdkafka queueing error, writing to fallback, msg ${this.stats.msgCnt}, ${err.message}, ${stringify(err)}`)
      log.error(error.message)

      if (err.code != null && err.code === RdKafka.CODES.ERRORS.ERR_MSG_SIZE_TOO_LARGE) {
        // messageTooLarge should not incr consecutiveKafkaErrorCnt
        // TODO write to fallback too large file
      } else {
        this.incrementConsecutiveKafkaErrorCnt()
        // TODO write to fallback
      }

      return Promise.reject(error)
    }
  }

  // how many messages are queued pending kafka ack (deliveryReport), error, or timeout to occur
  pending() {
    return Object.keys(this.q).length
  }

  queueMsgAckTimeout(topic, msgValue, key, ackKey) {
    const queuedMsg = {
      ackKey, // unique seq for kafka delivery-report to ack this message
      topic,
      msgValue, // the message to queue to kafka
      promise: {
        promise: null, // the promise for awaiting completion
        resolve: null, // resolve function for resolving promise on delivery-report ack
        reject: null, // reject function for rejecting promise on error or timeout waiting for delivery-report ack
        settled: false, // promises don't have a state variable, used to avoid race with mesg timeout
      },
      timoutObj: null, // timeout object that will handle timeout if delivery-report ack not received, cancelled on receive
      settleAndRemove: null, // clear timeout, set settled true, remove from pending, if error, reject promise, else resolve promise, return promise
    }

    queuedMsg.promise.promise = new Promise((resolve, reject) => {
      queuedMsg.promise.resolve = resolve
      queuedMsg.promise.reject = reject
    })

    queuedMsg.timoutObj = setTimeout((qdMsg) => {
      try {
        if (qdMsg.settled) return
        this.incrementConsecutiveKafkaErrorCnt() // will initiate reconnect upon consecutiveKafkaErrorCntReconnectThreshold
        queuedMsg.promise.settled = true
        delete this.q[qdMsg.ackKey]
        const error = new Error(`timeout, write to fallback, msg ${qdMsg.ackKey}`)
        queuedMsg.promise.reject(error)
        // TODO write to fallback
        log.warn(error.message)
      } catch (err) {
        // TODO warn/avoid pending leak if here, ??? just allow reconnect to clear upon consecutiveKafkaErrorCntReconnectThreshold
        log.error(err, 'Error executing mesg timeout, msg %d', qdMsg.ackKey)
      }
    }, this.options.producer['message.timeout.ms'], queuedMsg)

    // Clients responsibility to try/catch
    queuedMsg.settleAndRemove = (error, deliveryReport) => {
      if (queuedMsg.promise.settled) return queuedMsg.promise.promise
      clearTimeout(queuedMsg.timoutObj)
      queuedMsg.promise.settled = true
      delete this.q[queuedMsg.ackKey]
      if (error == null) {
        queuedMsg.promise.resolve(deliveryReport)
      } else {
        queuedMsg.promise.reject(error)
      }
      return queuedMsg.promise.promise
    }

    this.q[ackKey] = queuedMsg
    return queuedMsg
  }

  deliveryReport(error, deliveryReport) {
    try {
      this.stats.ackCnt += 1

      if (error != null) {
        this.incrementConsecutiveKafkaErrorCnt()
      // TODO handle error, cancel timeout, log, write to fallback
      // log.info('deliveryReport **************', error, report.opaque, this.pending())
      }

      this.consecutiveKafkaErrorCnt = 0

      const queuedMsg = this.q[deliveryReport.opaque]
      if (!queuedMsg) return
      queuedMsg.settleAndRemove(error, deliveryReport)
    } catch (err) {
      // TODO handle error, log, potential memory leak since it might not be cleaning up
      log.error(err, err, deliveryReport) // TODO FIXME
    }
  }

  // TODO need more specific error type counts, i.e. queue, messageTooLarge, ready vs notReady errorCnt
  incrementConsecutiveKafkaErrorCnt() {
    this.consecutiveKafkaErrorCnt += 1
    if (this.consecutiveKafkaErrorCnt >= this.options.consecutiveKafkaErrorCntReconnectThreshold) {
      this.kafkaReady = false
      // TODO fallback all pending messages, need to cancel timeouts so they don't fail after reconnect and cause loop
      this.startKafkaConnectRetryProcess()
    }
  }

  getStatistics() {
    this.stats.kafkaReady = this.kafkaReady
    this.stats.consecutiveKafkaErrorCnt = this.consecutiveKafkaErrorCnt
    this.stats.kafkaEventErrorCnt = this.kafkaEventErrorCnt
    this.stats.lastKafkaEventError = this.lastKafkaEventError == null ? null : this.lastKafkaEventError.message
    const stats = lodash.cloneDeep(this.stats)
    return stats
  }

  resetStatistics() {
    this.stats.ackCnt = 0 // messages successfully sent to kafka
    this.stats.errorCnt = 0 // total errors since start/reset, includes all retries
    this.stats.lastErrorTs = undefined // 'ISO8601'
    this.stats.lastError = undefined // error message
    this.stats.lastStatsReset = new Date().toUTCString() // startup time
    this.stats.filePublisher.resetStatistics()
  }

  updateErrorStatsInternal(err) {
    this.stats.errorCnt += 1
    this.stats.lastError = err.message
    this.stats.lastErrorTs = new Date().toUTCString()
  }
}
