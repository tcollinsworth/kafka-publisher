import { serial as test } from 'ava'
import sinon from 'sinon'
import delay from 'delay'
import waitUntil from 'async-wait-until'
import stringify from 'json-stringify-safe'
import lodash from 'lodash'

import * as k from '../index'

const testRetryOptions = {
  retries: null, // not strictly required, however disables creating default retry table
  // retries: 10000, // 10K ~2 months - creates a retry schedule for all retries (rediculous, why not computing) 8 9's causes FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory
  forever: true, // use this instead of retries or it will create a lookup table for all retries wasting cycles and memory
  factor: 2,
  minTimeout: 1, // 1 sec
  maxTimeout: 1, // 10 sec
  randomize: true,
}

let kafkaOptions
let fallbackOptions

const mockKafkaProducer = {
  send: sinon.stub(),
  init: sinon.stub(),
  shutdown: sinon.stub(),
  setPollInterval: sinon.stub(),
  on: sinon.stub(),
  connect: sinon.stub(),
  disconnect: sinon.stub(),
  produce: sinon.stub(),
}

const mockFallbackPublisher = {
  getStatistics: sinon.stub(),
  publish: sinon.stub(),
  publishOrphanedAck: sinon.stub(),
  publishMesgTooLarge: sinon.stub(),
  readyEvent: sinon.stub(),
}

k.kafka.initKafkaProducer = function (options) {
  kafkaOptions = options
  return mockKafkaProducer
}

k.fallback.initFallbackPublisher = function (options) {
  fallbackOptions = options
  return mockFallbackPublisher
}

test.beforeEach(() => {
  reset()
})

test.afterEach.always((t) => {
  reset()
})

function reset() {
  resetMockStubFunctions(mockKafkaProducer)
  kafkaOptions = undefined

  resetMockStubFunctions(mockFallbackPublisher)
  fallbackOptions = undefined
}

function resetMockStubFunctions(mock) {
  for (const p in mock) {
    if (mock[p].isSinonProxy) {
      mock[p].reset()
    }
  }
}

test('happy constructor and options', async (t) => {
  const options = {
    connectionString: 'testConnString',

    // producer
    defaultTopic: 'testTopic',

    producer: {
      'message.timeout.ms': 10101,
      dr_cb: false,
      event_cb: false,
    },

    producerPollIntervalMs: 55,

    // consecutive error threshold till reconnect is initiated
    // cleared on successful delivery report (kafka ack) and on transition to kafka ready
    consecutiveKafkaErrorCntReconnectThreshold: 11,
    kafkaReadyOrErrorOrTimeoutMs: 60001, // 1 min
    kafkaReadyOrErrorOrTimeoutPollMs: 101, // 100 ms

    retries: {
      attempts: 2,
      delay: {
        min: 333,
        max: 3333,
      },
    },

    retryOptions: {
      retries: 2, // not strictly required, however disables creating default retry table
      forever: false, // use this instead of retries or it will create a lookup table for all retries wasting cycles and memory
      factor: 1,
      minTimeout: 1001, // 1 sec
      maxTimeout: 10002, // 10 sec
      randomize: false,
    },

    // fallback defaults - where to write to filesystem
    fallback: {
      enabled: true,
      directory: 'kafkaFallbackLogs',
      retryOptions: {
        retries: 9,
        factor: 1,
        minTimeout: 222, // 0.1 sec
        maxTimeout: 2222, // 2 sec
        randomize: false,
      },
    },
  }
  const kp = new k.KafkaPublisher(options)

  t.deepEqual(options.producer, kp.options.producer)

  t.truthy(fallbackOptions.instanceId)
  const expectedOptions = lodash.cloneDeep(fallbackOptions)
  delete expectedOptions.instanceId

  t.deepEqual(options.fallback, expectedOptions)
})

test('constructor options undefined connectionString throws error', async (t) => {
  const options = { connectionString: undefined }
  try {
    const kp = new k.KafkaPublisher(options)
    t.fail('expected error')
  } catch (err) {
    t.pass()
  }
})

test('constructor options undefined throws error', async (t) => {
  const options = undefined
  try {
    const kp = new k.KafkaPublisher(options)
    t.fail('expected error')
  } catch (err) {
    t.pass()
  }
})

test('happy init', async (t) => {
  mockKafkaProducer.disconnect.onCall(0).callsArg(0)
  const kp = new k.KafkaPublisher({ connectionString: 'foo' })
  kp.init()

  // on ready
  kp.readyEvent()

  await waitUntil(() => !mockKafkaProducer.disconnect.called)
  await waitUntil(() => mockKafkaProducer.connect.called)
  await waitUntil(() => kp.getStatistics().kafkaReady)

  t.truthy(mockFallbackPublisher.readyEvent.called)
  // t.truthy(mockKafkaProducer.disconnect.calledBefore(mockKafkaProducer.connect))
  t.truthy(mockKafkaProducer.connect.called)

  t.is(k.defaultOptions.producerPollIntervalMs, mockKafkaProducer.setPollInterval.args[0][0])
  t.is(5, mockKafkaProducer.on.args.length)
})

test('one error, happy init 1', async (t) => {
  const kp = new k.KafkaPublisher({ connectionString: 'foo', kafkaReadyOrErrorOrTimeoutMs: 1000, retryOptions: testRetryOptions })
  kp.init()
  await delay(10)

  // on event.error
  mockKafkaProducer.on.args[2][1](new Error('test'))
  await delay(10)

  // on ready
  mockKafkaProducer.on.args[1][1]()

  await waitUntil(() => kp.getStatistics().kafkaReady)

  t.truthy(kp.getStatistics().kafkaReady)
  t.is(1, kp.getStatistics().kafkaEventErrorCnt)
})

test('one error, happy init 2', async (t) => {
  mockKafkaProducer.connect.onCall(0).throws(new Error('test'))

  const kp = new k.KafkaPublisher({ connectionString: 'foo', kafkaReadyOrErrorOrTimeoutMs: 1000, retryOptions: testRetryOptions })
  kp.init()
  await delay(10)

  // on ready
  mockKafkaProducer.on.args[1][1]()

  await waitUntil(() => kp.getStatistics().kafkaReady)

  t.truthy(kp.getStatistics().kafkaReady)
  t.is(0, kp.getStatistics().kafkaEventErrorCnt)
})

test('happy shutdown', async (t) => {
  const kp = new k.KafkaPublisher({ connectionString: 'foo' })
  kp.init()
  await delay(10)

  kp.shutdown()

  await waitUntil(() => mockKafkaProducer.disconnect.args.length > 0)

  t.pass()
})

test('happy queue and publishing to kafka', async (t) => {
  mockKafkaProducer.produce.onCall(0).returns(true)

  const kp = new k.KafkaPublisher({ connectionString: 'foo', defaultTopic: 'testTopic' })
  kp.init()
  await delay(10)

  // on ready
  mockKafkaProducer.on.args[1][1]()

  await waitUntil(() => kp.getStatistics().kafkaReady)

  const mesg = { foo: 'bar' }
  kp.queue('key', mesg)

  await waitUntil(() => kp.getStatistics().mesgCnt > 0)
  await waitUntil(() => kp.getStatistics().queueCnt > 0)

  t.is('testTopic', mockKafkaProducer.produce.args[0][0])
  t.falsy(mockKafkaProducer.produce.args[0][1])
  t.deepEqual(mesg, JSON.parse(mockKafkaProducer.produce.args[0][2].toString()))
  t.deepEqual('key', mockKafkaProducer.produce.args[0][3])
  t.deepEqual(1, mockKafkaProducer.produce.args[0][5])
})

test('queue kafka send error fallsback and next send succeeds', async (t) => {
  mockKafkaProducer.produce.onCall(0).returns(false)
  mockKafkaProducer.produce.onCall(1).returns(true)

  const kp = new k.KafkaPublisher({ connectionString: 'foo', defaultTopic: 'testTopic' })
  kp.init()
  await delay(10)

  // on ready
  mockKafkaProducer.on.args[1][1]()

  await waitUntil(() => kp.getStatistics().kafkaReady)

  const mesg1 = { foo: 'bar1' }
  kp.queue('key1', mesg1)
  const mesg2 = { foo: 'bar2' }
  kp.queue('key2', mesg2)

  await waitUntil(() => kp.getStatistics().mesgCnt > 1)
  await waitUntil(() => kp.getStatistics().ackErrorCnt > 0)
  await waitUntil(() => kp.getStatistics().queueCnt > 0)

  t.is(0, kp.getStatistics().ackSuccessCnt)

  t.is(1, mockFallbackPublisher.publish.args.length)

  const expectedFileSendMockArgs = [
    {
      topic: 'testTopic',
      mesgValue: stringify(mesg1),
      ackKey: 1,
    },
  ]

  await waitUntil(() => lodash.isEqual(mockFallbackPublisher.publish.args[0], expectedFileSendMockArgs))

  t.is('testTopic', mockKafkaProducer.produce.args[1][0])
  t.falsy(mockKafkaProducer.produce.args[1][1])
  t.deepEqual(mesg2, JSON.parse(mockKafkaProducer.produce.args[1][2].toString()))
  t.deepEqual('key2', mockKafkaProducer.produce.args[1][3])
  t.deepEqual(2, mockKafkaProducer.produce.args[1][5])
})

// TODO queue no defaultTopic
// TODO publishInternal
// TODO kafkaSendFallsbackWhileDownInternal
// TODO kafka message too large
// TODO kafka topic override
// TODO publish timedout, kafka deliveryReport orphaned
// TODO getStatistics, resetStatistics
