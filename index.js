// eslint-disable-next-line no-global-assign
require = require('esm')(module/* , options */)
// point the following at ./lib/kafka-publisher for es6
module.exports = require('./lib/kafka-publisher2') // es6
// point the following at ./dist/kafka-publisher for transpiled es5
// TODO switch back to ES5
// module.exports = require('./dist/kafka-publisher') // es5
