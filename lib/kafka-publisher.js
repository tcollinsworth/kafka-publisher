import Kafka from 'no-kafka'
import promiseRetry from 'promise-retry'
import uuidv4 from 'uuid/v4'
import stringify from 'json-stringify-safe'
import { getLogger } from './logging'
import { FilePublisher } from './file-publisher'

uuidv4()

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

export class KafkaPublisher {
  constructor(options) {
    if (options == null || options.connectionString == null) {
      throw new Error('KafkaPublisher options.connectionString must be set with kafka seed broker list, i.e., 127.0.0.1:9092,...')
    }

    this.id = uuidv4()
    this.kafkaReady = false
    this.publising = false
    this.q = []
    this.options = Object.assign(defaultOptions, options)
    this.kafkaProducer = new Kafka.Producer(this.options)
    this.fallback = new FilePublisher(this.options.fallback)
    this.stats = {
      queueCnt: 0,
      sentCnt: 0,
      errorCnt: 0,
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
        this.updateErrorStats(err)
        log.error('Error initializing kafka client', err)
        retry(err)
      }
    }, this.retryOptions)
  }

  async end() {
    return this.kafkaProducer.end()
  }

  // value = message
  queue(key, value, topic = this.options.defaultTopic) {
    if (topic == null) {
      const err = new Error('Error defaultTopic or per message queue topic not specified, discarding')
      this.updateErrorStats(err)
      log.error(err.message)
      return
    }
    // eslint-disable-next-line no-plusplus
    ++this.stats.queueCnt
    this.q.push({ topic, key, value })
    setImmediate(this.handleQueued.bind(this))
  }

  async handleQueued() {
    if (this.publishing || this.q.length < 1) return

    try {
      this.publishing = true // only want one loop/continuation handling/mutating/publishing from queue
      while (this.q.length > 0) {
        const mesg = this.q[this.q.length - 1]
        if (this.kafkaReady) {
          // eslint-disable-next-line no-await-in-loop
          if (await this.publish(mesg.topic, mesg.key, mesg.value)) {
            // eslint-disable-next-line no-plusplus
            ++this.stats.sentCnt
            this.q.pop() // remove last element from array
            // eslint-disable-next-line no-continue
            continue
          }
        }
        // eslint-disable-next-line no-await-in-loop
        await this.fallback.send(mesg) // best effort with retries
        this.q.pop() // remove last element from array
      }
    } catch (err) {
      log.error(`Error handling queued ${err.message}`, err)
    } finally {
      this.publishing = false
    }
  }

  async publish(topic, key, value) {
    const kafkaMesg = { topic, message: { key: stringify(key), value: stringify(value) } }
    try {
      try {
        validateKey(key)
        validateValue(value)
      } catch (err) {
        log.error(`Error validating message key or value ${err.message}, ${stringify(key)}, ${stringify(value)}`, err)
        return true
      }

      const resp = await this.kafkaProducer.send(kafkaMesg)
      log.debug(stringify(resp), stringify(kafkaMesg))

      if (resp == null || resp.length < 1 || resp[0].error != null) {
        // if message too large error, log and skip, do NOT set kafkaReady false
        if (resp != null && resp.length > 0 && resp[0].error != null && resp[0].error.code === 'MessageSizeTooLarge') {
          log.error(`Kafka MessageSizeTooLarge (default > 1 MB), discarding ${stringify(kafkaMesg)}`)
          return true
        }
        this.kafkaReady = false
        // TODO start background process to keep retrying until kafka recovers

        log.error(`Error publishing to kafka ${stringify(resp)}, ${stringify(kafkaMesg)}`)
        return false
      }
      return true
    } catch (err) {
      log.error(`Error publishing to kafka ${err.message}, ${stringify(kafkaMesg)}`, err)
      return false
    }
  }

  queued() {
    return this.q.length
  }

  getStatistics() {
    return this.stats
  }

  updateErrorStats(err) {
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
