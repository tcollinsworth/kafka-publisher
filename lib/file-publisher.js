import promiseRetry from 'promise-retry'
import mkdirp from 'mkdirp'
import moment from 'moment'

import stringify from 'json-stringify-safe'
import { getLogger } from './logging'

const util = require('util')
const fs = require('fs')

const appendFile = util.promisify(fs.appendFile)

let log

const defaultOptions = {
  // instanceId: <uuid>, //passed from kafka-publisher on construction
  // enabled: true,

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
}

export function initLogger() {
  log = getLogger()
}

export class FilePublisher {
  constructor(options) {
    this.options = Object.assign(defaultOptions, options)

    if (this.options == null || this.options.directory == null) {
      throw new Error('FilePublisher options.directory must be set')
    }

    this.stats = {
      readySeq: 0,
      mesgCnt: 0,
      errorCnt: 0,
      lastErrorTs: undefined, // 'ISO8601'
      lastError: undefined, // error message
      lastReset: new Date().toUTCString(), // startup time
    }

    if (!fs.existsSync(this.options.directory)) {
      log.error(`Error fallback directory ${this.options.directory} does not exist, fallback will not work and messages will be lost forever`)
      this.updateErrorStatsInternal(new Error(`Error fallback directory ${this.options.directory} does not exist`))
      this.attempCreateDirInternal(this.options.directory)
    }

    if (!this.options.enabled) {
      log.warn('Fallback NOT enabled, messages will be lost forever if kafka unavailable, no delivery-report (ack) received from kafka and/or message timeouts')
    }
  }

  // implement log rolling, i.e., start of every kafka ready event and/or hourly
  // topic in filename so processable per topic
  // new file hourly yyyymmddhh, small sortable
  // new file on each kafka ready event, so they can be processed sooner

  // dir/<topic>~<yyyymmddhh>~<instanceUuid>~<instanceReadySeq>.fallback.log
  // dir/<topic>~<yyyymmddhh>~<instanceUuid>~<instanceReadySeq>.largeMesg.fallback.log
  getFallBackLogFileName(topic, isMesgTooLarge = false) {
    const yyyymmddhh = moment.utc().format('YYYYMMDDhh')
    if (!isMesgTooLarge) {
      return `${this.options.directory}/${topic}~${yyyymmddhh}~${this.options.instanceId}~${this.stats.readySeq}.fallback.log`
    }
    return `${this.options.directory}/${topic}~${yyyymmddhh}~${this.options.instanceId}~${this.stats.readySeq}.largeMesg.fallback.log`
  }

  async readyEvent() {
    this.stats.readySeq += 1
  }

  // mesg = {topic, mesgValue}
  async publish(mesg) {
    if (!this.options.enabled) return
    const fallbackLogFileName = this.getFallBackLogFileName(mesg.topic)
    this.publishInternal(fallbackLogFileName, mesg)
  }

  // mesg = {topic, mesgValue}
  async publishMesgTooLarge(mesg) {
    if (!this.options.enabled) return
    const fallbackLogFileName = this.getFallBackLogFileName(mesg.topic, true)
    this.publishInternal(fallbackLogFileName, mesg)
  }

  async publishInternal(fallbackLogFileName, mesg) {
    this.stats.mesgCnt += 1

    await promiseRetry(async (retry) => {
      try {
        if (log.isLevelEnabled('debug')) log.debug('FilePublisher.send')
        await this.appendToFileInternal(fallbackLogFileName, stringify(mesg))
      } catch (err) {
        this.updateErrorStatsInternal(err)
        log.error(err, 'Error appending to file, retrying')
        retry(err)
      }
    }, this.options.retryOptions)
  }

  // TODO consider/test using open and append to file descriptor if it's more performance/efficient

  // eslint-disable-next-line class-methods-use-this
  async attempCreateDirInternal(dir) {
    try {
      mkdirp.sync(dir)
      log.warn(`Created fallback directory ${dir}, if ephemperal, i.e., Docker, upon crash or restart messages will be lost forever`)
    } catch (err) {
      log.error(`Error creating fallback directory ${dir} does not exist, fallback will not work and messages will be lost forever`)
    }
  }

  // eslint-disable-next-line class-methods-use-this
  async appendToFileInternal(fallbackLogFileName, mesg) {
    await appendFile(fallbackLogFileName, `${mesg}\n`, 'utf8')
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
    this.stats.errorCnt += 1
    this.stats.lastError = err.message
    this.stats.lastErrorTs = new Date().toUTCString()
  }
}
