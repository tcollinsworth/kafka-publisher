import promiseRetry from 'promise-retry'

import stringify from 'json-stringify-safe'
import { getLogger } from './logging'

const util = require('util')
const fs = require('fs')

const appendFile = util.promisify(fs.appendFile)

const log = {
  error: getLogger('error'),
  warn: getLogger('warn'),
  info: getLogger('info'),
  debug: getLogger('debug'),
}

const defaultOptions = {
  // /home/troy/development/node/kafka-publisher/
  directory: 'kafkaFallbackLogs', // non-ephemeral filesystem mount, shared by all nodes
  retryOptions: {
    retries: 5, // not strictly required, however disables creating default retry table
    // retries: 10000, // 10K ~2 months - creates a retry schedule for all retries (rediculous, why not computing) 8 9's causes FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory
    // forever: true, // use this instead of retries or it will create a lookup table for all retries wasting cycles and memory
    factor: 2,
    minTimeout: 100, // 0.1 sec
    maxTimeout: 2000, // 2 sec
    randomize: true,
  },
}

export class FilePublisher {
  constructor(options) {
    this.options = Object.assign(defaultOptions, options)

    if (this.options == null || this.options.directory == null) {
      throw new Error('FilePublisher options.directory must be set')
    }

    this.options.fallbackLog = `${this.options.directory}/fallback-${this.options.id}.log`

    this.stats = {
      mesgCnt: 0,
      errorCnt: 0,
      lastErrorTs: undefined, // 'ISO8601'
      lastError: undefined, // error message
      lastReset: new Date().toUTCString(), // startup time
    }

    if (!fs.existsSync(this.options.directory)) {
      log.error(`Error fallback directory ${this.options.directory} does not exist, fallback will not work and messages will be lost forever`)
      this.updateErrorStatsInternal(new Error(`Error fallback directory ${this.options.directory} does not exist`))
    }
  }

  async send(mesg) {
    // eslint-disable-next-line no-plusplus
    ++this.stats.mesgCnt
    await promiseRetry(async (retry) => {
      try {
        log.debug('FilePublisher.send', mesg)
        await this.appendToFileInternal(stringify(mesg))
      } catch (err) {
        this.updateErrorStatsInternal(err)
        log.error('Error appending to file, retrying', err)
        retry(err)
      }
    }, this.options.retryOptions)
  }

  //TODO implement log rolling, i.e., daily or hourly
  //TODO use open and append to file descriptor if it's more efficient

  async appendToFileInternal(mesg) {
    await appendFile(this.options.fallbackLog, `${mesg}\n`, 'utf8')
  }

  getStatistics() {
    return this.stats
  }

  resetStatistics() {
    this.stats.mesgCnt = 0
    this.stats.errorCnt = 0
    this.stats.lastErrorTs = undefined // 'ISO8601'
    this.stats.lastError = undefined // error message
    this.stats.lastReset = new Date().toUTCString() // startup time
  }

  updateErrorStatsInternal(err) {
    // eslint-disable-next-line no-plusplus
    ++this.stats.errorCnt
    this.stats.lastError = err.message
    this.stats.lastErrorTs = new Date().toUTCString()
  }
}
