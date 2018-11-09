import { serial as test } from 'ava'
import sinon from 'sinon'
import delay from 'delay'

import * as k from '../index'

const mockKafkaProducer = {
  send: sinon.stub(),
  init: sinon.stub(),
  end: sinon.stub(),
}

k.kafka.initKafkaProducer = function() {
  return mockKafkaProducer
}

test.beforeEach(() => {
  reset()
})

test.afterEach.always(t => {
  reset()
})

function reset() {
  resetMockStubFunctions(k.kafka.Producer)
}

function resetMockStubFunctions(mock) {
  for(const p in mock) {
    if (mock[p].isSinonProxy) {
      mock[p].reset()
    }
  }
}

//TODO construct
//TODO init
//TODO end
//TODO queue
//TODO queue no defaultTopic
//TODO queueMessages
//TODO queueMessages no defaultTopic
//TODO getStatistics, resetStatistics
//TODO validateKey
//TODO validateValue
//TODO handleQueued
//TODO retry
//TODO fallback
//TODO kafka success
//TODO kafka message too large
//TODO kafka no topic
//TODO kafka perpetual failure
//TODO kafka failure, then recovery

test('queue message', async t => {
  await delay(1000)
  t.is(true, true)
})

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