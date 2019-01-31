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
  await delay(3000)
  let cnt = 0;
  while (true) {
    // const foo = {
    //   userId: 'userId'
    // }
    // const buf1 = new Buffer('test', 'utf8')
    // //kp.queue(buf1, { foo: 'buf1' }) // use defaultTopic
    // kp.queue(buf1.toString(), { foo: 'buf1.toString()' }) // use defaultTopic
    //
    // //const buf2 = Buffer.from('test', 'utf8')
    // //kp.queue(buf2, { foo: 'buf2' }) // use defaultTopic
    //
    // //kp.queue('"key"', { foo: 'bar0' }) // use defaultTopic
    // kp.queue('key', { foo: 'bar1' }) // use defaultTopic
    // kp.queue(uuidV4(), { foo: 'bar2' }) // use defaultTopic
    // kp.queue(uuidV4().toString(), { foo: 'bar3' }) // use defaultTopic
    // // kp.queue(stringify(stringify(uuidV4())), { foo: 'bar4' }) // use defaultTopic
    // kp.queue(foo.userId, { foo: 'bar5' }) // use defaultTopic
    // kp.queueMessages([
    //   { key: 'key1', value: { foo1: 'bar1' } },
    //   { key: 'key2', value: { foo2: 'bar2' } },
    // ]) // use defaultTopic

    if (kp.queued() < 1000) {
    kp.queue(uuidV4(), {
       "corrId":"2db82d53-d238-4b8f-889a-19916497fcf9",
       "orgId":"f218de12-9565-4929-94ab-75fec8decc1c",
       "byUserId":"8d0a5168-e3f6-45b1-b0c6-798981df27eb",
       "msgBody":{
          "contentUuid":"9cd587f8-5ff6-4b35-b33b-c7703dc5b8e6",
          "contentType":"AUDIOBOOK",
          "sessionId":"5e0471e1-eabf-4a4f-8311-fe1b485055ba",
          "action":"CONSUMED",
          "actionCtx":{
             "pointUnit":"SECONDS",
             "startPoint":"14909",
             "endPoint":"14914",
             "playbackMultiplier":1,
             "durationInSeconds":5,
             "sectionKey":"https://cdn2.percipio.com/1548813056.8213b9a82cf17f926466322e3cbb9f4bcbeac0a0/eod/books/134942/downloadmedia/Books24x7-Listen_Up_or_Lose_Out-Audio.mp4",
             "parentUuid":null
          },
          "actionCtxSchema":"launched~2018100517120000~v1~Schema.json"
       },
       "schema":"root~v2.1",
       "id":"f5c8c324-0133-4a9a-91f0-0a7da5780ac0",
       "time":"2019-01-29T06:03:05.773Z",
       "source":"LP:Front-BFF",
       "msgSchema":"learnerActivity~2018100517120000~v1.0",
       "msgClass":"event"
    }) // use defaultTopic
    ++cnt;
    console.log(cnt, 'q', kp.queued())
  } else {
    while (kp.queued() > 500) {
      await delay(1000)
      console.log(cnt, 'q', kp.queued())
    }
  }

    //await delay(500)
    // if (cnt % 10000 == 0) console.log(cnt, kp.getStatistics())
    //console.log(kp.getStatistics())
    // await delay(10)
    // console.log(cnt, 'q', kp.queued())
    //console.log('awaiting 10 min for connection timeout')
    //await delay(600000)
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
