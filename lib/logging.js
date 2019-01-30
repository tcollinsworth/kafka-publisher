import debug from 'debug'
// import stdoutStream from 'stdout-stream'

// eslint-disable-next-line no-console
// const asyncConsole = new console.Console(stdoutStream, stdoutStream)

export function getLogger(name) {
  const log = debug(`kafka-publisher:${name}`)
  // log.log = asyncConsole.log.bind(asyncConsole)
  return log
}
