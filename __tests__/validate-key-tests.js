import test from 'ava'
import uuidV4 from 'uuid/v4'
import stringify from 'json-stringify-safe'

import { validateKey } from '../lib/validate-key'

test('uuid returns uuid', t => {
  const uuid = uuidV4()
  t.is('string', typeof uuid)
  t.is(uuid, validateKey(uuid))
})

test('uuid toString returns uuid', t => {
  const uuid = uuidV4().toString()
  t.is(uuid, validateKey(uuid))
})

test('stringify uuid returns quoted uuid', t => {
  const uuid = stringify(uuidV4())
  t.is(uuid, validateKey(uuid))
  t.truthy(uuid.startsWith('"'))
  t.truthy(uuid.endsWith('"'))
})

test('number returns number', t => {
  t.is(42, validateKey(42))
})

test('boolean throws error', t => {
  const error = t.throws(() => validateKey(true), Error)
  t.is(error.message, 'key should be a number, string, or object, was boolean')
})

test('array returns stringified array', t => {
  t.is('[]', validateKey([]))
})

test('object returns stringified object', t => {
  t.is('{}', validateKey({}))
})

test('null throws error', t => {
  const error = t.throws(() => validateKey(null), Error)
  t.is(error.message, 'key should be a number, string, or object, was object')
})

test('undefined throws error', t => {
  const error = t.throws(() => validateKey(undefined), Error)
  t.is(error.message, 'key should be a number, string, or object, was undefined')
})

test('function throws error', t => {
  const error = t.throws(() => validateKey(() => {}), Error)
  t.is(error.message, 'key should be a number, string, or object, was function')
})

test('symbol throws error', t => {
  const error = t.throws(() => validateKey(Symbol('foo')), Error)
  t.is(error.message, 'key should be a number, string, or object, was symbol')
})
