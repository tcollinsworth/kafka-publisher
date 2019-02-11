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
  maxTimeout: 10, // 10 sec
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
}

const mockFallbackPublisher = {
  getStatistics: sinon.stub(),
  publish: sinon.stub(),
}

k.kafka.initKafkaProducer = function(options) {
  kafkaOptions = options
  return mockKafkaProducer
}

k.fallback.initFallbackPublisher = function(options) {
  fallbackOptions = options
  return mockFallbackPublisher
}

test.beforeEach(() => {
  reset()
})

test.afterEach.always(t => {
  reset()
})

function reset() {
  resetMockStubFunctions(mockKafkaProducer)
  kafkaOptions = undefined

  resetMockStubFunctions(mockFallbackPublisher)
  fallbackOptions = undefined
}

function resetMockStubFunctions(mock) {
  for(const p in mock) {
    if (mock[p].isSinonProxy) {
      mock[p].reset()
    }
  }
}

test('happy constructor and options', async t => {
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

  t.deepEqual(options.producer, kafkaOptions)

  t.truthy(fallbackOptions.instanceId)
  const expectedOptions = lodash.cloneDeep(fallbackOptions)
  delete expectedOptions.instanceId

  t.deepEqual(options.fallback, expectedOptions)
})

test('constructor options undefined connectionString throws error', async t => {
  const options = { connectionString: undefined }
  try {
    const kp = new k.KafkaPublisher(options)
    t.fail('expected error')
  } catch (err) {
    t.pass()
  }
})

test('constructor options undefined throws error', async t => {
  const options = undefined
  try {
    const kp = new k.KafkaPublisher(options)
    t.fail('expected error')
  } catch (err) {
    t.pass()
  }
})

test('happy init', async t => {
  const kp = new k.KafkaPublisher({connectionString: 'foo'})
  kp.init()

  await waitUntil(() => kp.getStatistics().kafkaReady)
  t.is(1, mockKafkaProducer.init.args.length)
})

test('one error, happy init', async t => {
  mockKafkaProducer.init.onCall(0).returns(Promise.reject(new Error('test')))

  const kp = new k.KafkaPublisher({connectionString: 'foo', retryOptions: testRetryOptions})
  kp.init()

  await waitUntil(() => kp.getStatistics().kafkaReady)
  await waitUntil(() => mockKafkaProducer.init.args.length > 1)
  t.is(2, mockKafkaProducer.init.args.length)
  t.is('test', kp.getStatistics().lastError)
  t.truthy(kp.getStatistics().lastErrorTs)
})

test('happy end', async t => {
  const kp = new k.KafkaPublisher({connectionString: 'foo'})
  kp.end()

  await waitUntil(() => mockKafkaProducer.end.args.length > 0)
})

test('happy queue and publishing to kafka', async t => {
  mockKafkaProducer.send.onCall(0).returns(Promise.resolve([ { topic: 'testTopic', partition: 0, error: null, offset: 1 } ]))

  const kp = new k.KafkaPublisher({connectionString: 'foo', defaultTopic: 'testTopic',})
  kp.init()

  await waitUntil(() => kp.getStatistics().kafkaReady)

  const mesg = { foo: 'bar' }
  kp.queue('key', mesg)

  await waitUntil(() => kp.getStatistics().queueCnt > 0)
  await waitUntil(() => kp.getStatistics().sentCnt > 0)
  await waitUntil(() => mockKafkaProducer.send.args.length > 0)

  t.is('testTopic', mockKafkaProducer.send.args[0][0].topic)
  t.deepEqual('key', mockKafkaProducer.send.args[0][0].message.key)
  t.deepEqual(mesg, JSON.parse(mockKafkaProducer.send.args[0][0].message.value))
})

test('queue kafka send error recovers and next send succeeds', async t => {
  const kafkaErrorResp = [
    { topic: 'testTopic',
      partition: -1,
      error:
       { name: 'KafkaError',
         code: 'SomeErrorCode',
         message: 'Some error message' },
      offset: -1
    }
  ]
  mockKafkaProducer.send.onCall(0).returns(Promise.resolve(kafkaErrorResp))
  mockKafkaProducer.send.returns(Promise.resolve([ { topic: 'testTopic', partition: 0, error: null, offset: 1 } ]))

  const kp = new k.KafkaPublisher({connectionString: 'foo', defaultTopic: 'testTopic', retryOptions: testRetryOptions})
  kp.init()

  await waitUntil(() => kp.getStatistics().kafkaReady)

  const mesg = { foo: 'bar' }
  kp.queue('key', mesg)

  t.is(0, mockFallbackPublisher.send.args.length)

  await waitUntil(() => kp.getStatistics().queueCnt > 0)
  await waitUntil(() => kp.getStatistics().kafkaReady)
  await waitUntil(() => !kp.getStatistics().backgroundRetrying)

  const expectedFileSendMockArgs = [
    {
      topic: 'testTopic',
      key: 'key',
      value: mesg
    }
  ]

  await waitUntil(() => lodash.isEqual(mockFallbackPublisher.send.args[0], expectedFileSendMockArgs))

  kp.queue('key', mesg)

  await waitUntil(() => kp.getStatistics().sentCnt > 0)
  await waitUntil(() => mockKafkaProducer.send.args.length > 0)

  t.is('testTopic', mockKafkaProducer.send.args[0][0].topic)
  t.deepEqual('key', mockKafkaProducer.send.args[0][0].message.key)
  t.deepEqual(mesg, JSON.parse(mockKafkaProducer.send.args[0][0].message.value))
})

//TODO queue no defaultTopic
//TODO queueMessages no defaultTopic


//TODO queueMessages
//TODO getStatistics, resetStatistics
//TODO handleQueued
//TODO publishInternal
//TODO kafkaSendBlocksWhileDownInternal
//TODO kafka message too large
//TODO kafka topic override, queue, queueMessages

// success
// [ { topic: 'test-topic', partition: 0, error: null, offset: 35 } ]

// error message
// [ { topic: 'Provisioning-Audits',
//     partition: -1,
//     error:
//      { [KafkaError: This request is for a topic or partition that does not exist on this broker.]
//        name: 'KafkaError',
//        code: 'UnknownTopicOrPartition',
//        message: 'This request is for a topic or partition that does not exist on this broker.' },
//     offset: -1 } ]

// { [KafkaError: The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.]
//   name: 'KafkaError',
//   code: 'MessageSizeTooLarge',
//   message: 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.' } }
