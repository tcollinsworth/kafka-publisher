import test from 'ava'
import uuidV4 from 'uuid/v4'
import stringify from 'json-stringify-safe'

import { validateValue } from '../lib/validate-value'

test('number throws error', t => {
  const error = t.throws(() => validateValue(42), Error)
  t.is(error.message, 'value should be an object, was number')
})

test('boolean throws error', t => {
  const error = t.throws(() => validateValue(true), Error)
  t.is(error.message, 'value should be an object, was boolean')
})

test('array does not throw error', t => {
  validateValue([])
})

test('object does not throw error', t => {
  t.is(validateValue({}))
})

test('null throws error', t => {
  const error = t.throws(() => validateValue(null), Error)
  t.is(error.message, 'value should be an object, was object')
})

test('undefined throws error', t => {
  const error = t.throws(() => validateValue(undefined), Error)
  t.is(error.message, 'value should be an object, was undefined')
})

test('function throws error', t => {
  const error = t.throws(() => validateValue(() => {}), Error)
  t.is(error.message, 'value should be an object, was function')
})

test('symbol throws error', t => {
  const error = t.throws(() => validateValue(Symbol('foo')), Error)
  t.is(error.message, 'value should be an object, was symbol')
})
