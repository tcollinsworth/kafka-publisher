// eslint-disable-next-line no-global-assign
// require = require('esm')(module/* , options */)
// module.exports = require('./lib/kafka-publisher.mjs') // es6

export {
  defaultOptions,
  kafka,
  fallback,
  KafkaPublisher,
} from './lib/kafka-publisher.mjs'
