import test from 'ava'
import { v4 as uuidV4 } from 'uuid'
import stringify from 'json-stringify-safe'

import { validateKey } from '../lib/validate-key'

test('uuid returns uuid', (t) => {
  const uuid = uuidV4()
  t.is('string', typeof uuid)
  t.is(uuid, validateKey(uuid))
})

test('uuid toString returns uuid', (t) => {
  const uuid = uuidV4().toString()
  t.is(uuid, validateKey(uuid))
})

test('stringify uuid returns quoted uuid', (t) => {
  const uuid = stringify(uuidV4())
  t.is(uuid, validateKey(uuid))
  t.truthy(uuid.startsWith('"'))
  t.truthy(uuid.endsWith('"'))
})

test('number returns number', (t) => {
  t.is(42, validateKey(42))
})

test('boolean throws error', (t) => {
  const error = t.throws(() => validateKey(true))
  t.is(error.message, 'key should be a number or string, was boolean')
})

test('array returns stringified array', (t) => {
  const error = t.throws(() => validateKey([]))
  t.is(error.message, 'key should be a number or string, was object')
})

test('object returns stringified object', (t) => {
  const error = t.throws(() => validateKey({}))
  t.is(error.message, 'key should be a number or string, was object')
})

test('null throws error', (t) => {
  const error = t.throws(() => validateKey(null))
  t.is(error.message, 'key should be a number or string, was object')
})

test('undefined throws error', (t) => {
  const error = t.throws(() => validateKey(undefined))
  t.is(error.message, 'key should be a number or string, was undefined')
})

test('function throws error', (t) => {
  const error = t.throws(() => validateKey(() => {}))
  t.is(error.message, 'key should be a number or string, was function')
})

test('symbol throws error', (t) => {
  const error = t.throws(() => validateKey(Symbol('foo')))
  t.is(error.message, 'key should be a number or string, was symbol')
})
