const baseConfig = require('./ava.config.cjs')

module.exports = {
  ...baseConfig,
  files: ['**/__integ-tests__/**/*test*.js'],
  timeout: '2m',
}
