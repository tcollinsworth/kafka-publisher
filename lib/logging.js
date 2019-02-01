import debug from 'debug'
// import stdoutStream from 'stdout-stream'

// eslint-disable-next-line no-console
// const asyncConsole = new console.Console(stdoutStream, stdoutStream)

// loggers must be created after they are enable or they won't log
// all filters must be on the same line, otherwise only the last enable call matched params are active
debug.enable('kafka-publisher:info,kafka-publisher:warn,kafka-publisher:error')

export function getLogger(name) {
  const log = debug(`kafka-publisher:${name}`)
  // log.log = asyncConsole.log.bind(asyncConsole)
  return log
}

// const logger = {
//   error: getLogger('error'),
//   warn: getLogger('warn'),
//   info: getLogger('info'),
//   debug: getLogger('debug'),
// }
//
// logger.info('info *******************************')
// logger.warn('warn *******************************')
// logger.error('error *******************************')
// logger.debug('debug *******************************')
