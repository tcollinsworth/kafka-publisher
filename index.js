// eslint-disable-next-line no-global-assign
require = require('esm')(module/* , options */)
// point the following at ./lib/kafka-publisher for es6
// module.exports = require('./lib/kafka-publisher')
// point the following at ./dist/kafka-publisher for transpiled es5
module.exports = require('./dist/kafka-publisher')