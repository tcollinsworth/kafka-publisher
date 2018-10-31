import { serial as test } from 'ava'
//import sinon from 'sinon'
import delay from 'delay'

import { KafkaPublisher } from '../lib/kafka-publisher'

let kp

// test.beforeEach(t => {
//
// })

function createKp(t) {
  kp = new KafkaPublisher({connectionString: '127.0.0.12:9092', defaultTopic: 'test-topic'})
  t.not(null, kp)
  t.is(0, kp.queued())
}

test.afterEach.always(async () => {
  if (kp != null) {
    await kp.end()
  }
})

test('queue message', async t => {
  createKp(t)
  kp.init() //retries forever, await blocks till ready, not desirable for fallback
  await delay(1000)
  while (true) {
    kp.queue('key', {foo: 'bar'}) // use defaultTopic
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

// Log and discard message if
//
//   * bad key - (null, undefined, Boolean, Symbol) must be (string, number, object)
//   * key too large
//   * bad value - not JSON object
//   * value too large

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
