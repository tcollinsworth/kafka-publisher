import { serial as test } from 'ava'
// import sinon from 'sinon'
import delay from 'delay'
import uuidV4 from 'uuid/v4'
import stringify from 'json-stringify-safe'

import { KafkaPublisher } from '../index'

let kp

// test.beforeEach(t => {
//
// })

function createKp(t) {
  kp = new KafkaPublisher({ connectionString: '127.0.0.12:9092', defaultTopic: 'test-topic' })
  t.not(null, kp)
  t.is(0, kp.queued())
}

test.afterEach.always(async () => {
  if (kp != null) {
    await kp.end()
  }
})

test('queue message', async (t) => {
  createKp(t)
  kp.init() // retries forever, await blocks till ready, not desirable for fallback
  await delay(1000)
  while (true) {
    kp.queue('key', { foo: 'bar1' }) // use defaultTopic
    kp.queueMessages([
      { key: 'key1', value: { foo1: 'bar1' } },
      { key: 'key2', message: { foo2: 'bar2' } },
    ]) // use defaultTopic
    await delay(500)
    console.log(kp.getStatistics())
    await delay(5000)
  }
  // await delay(1000)
  // kp.queue('key', {foo: 'bar'}) // use defaultTopic
  // t.is(1, kp.queued())
  // await delay(1000)
  // console.log(kp.getStatistics())
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
