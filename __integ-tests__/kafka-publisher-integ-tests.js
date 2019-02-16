import { serial as test } from 'ava'
// import sinon from 'sinon'
import delay from 'delay'
import uuidV4 from 'uuid/v4'
import stringify from 'json-stringify-safe'
import lodash from 'lodash'

import { KafkaPublisher } from '../index'

let kp
let cnt
let connected = false

// test.beforeEach(t => {
//
// })

function createKp(t) {
  kp = new KafkaPublisher({
    connectionString: '127.0.0.12:9092',
    defaultTopic: 'test-topic',
    kafkaReadyOrErrorOrTimeoutMs: 5000,
    // producer: {
    //   'message.timeout.ms': 1
    // },
    logging: {
      level: 'info', //'debug',
    },
    fallback: {
      //enabled: false,
    },
  })
  t.not(null, kp)
  //t.is(0, kp.queued())
}

test.afterEach.always(async () => {
  if (kp != null) {
    kp.shutdown()
  }
})

test('queue message', async (t) => {
  createKp(t)
  kp.init() // retries forever, await blocks till ready, not desirable for fallback
  connected = true
  await delay(3000)
  cnt = 0;
  // setInterval(() => {
  //   try {
  //     if (connected) {
  //       console.log('start disconnecting')
  //       kp.shutdown()
  //       connected = false
  //     } else {
  //       console.log('start connecting')
  //       kp.connect()
  //       connected = true
  //     }
  //   } catch (err) {
  //     console.log('Error', connected, err)
  //   }
  //
  // }, 30000)
  while (true) {
    //if (kp.pending() > 100) console.log('kp.pending', kp.pending())
    while (kp.pending() > 0) {
      await delay(100)
    }
    await _send()
    await delay(1000)
  }
})

function getTooLargeMesg() {
  const tmpMsg = lodash.cloneDeep(mesgValue)

  tmpMsg.foo = []
  for (let i = 0; i< 5000; i++) {
    tmpMsg.foo.push(lodash.cloneDeep(mesgValue))
  }

  return tmpMsg
}

async function _send() {
  if (++cnt % 1 == 0) console.log('send conn', connected, 'cnt', cnt, 'consecErrCnt', kp.getStatistics().consecutiveKafkaErrorCnt)

  kp.queue(uuidV4(), mesgValue, null, cb)
  //kp.queue(uuidV4(), getTooLargeMesg(), null, cb)
}

async function cb(error, deliveryReport) {
  if (cnt % 1 == 0) console.log('cb dr', deliveryReport == null ? null : deliveryReport.opaque, error.message)
  //await _send()
}

const mesgValue = {
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
}
