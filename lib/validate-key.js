import stringify from 'json-stringify-safe'

// discard message if bad key - (null, undefined, Boolean, Symbol) must be (string, number, object)
export function validateKey(key) {
  if (key == null || typeof key === 'boolean' || typeof key === 'symbol' || typeof key === 'function') {
    throw new Error(`key should be a number, string, or object, was ${typeof key}`)
  }

  if (typeof key === 'string' || typeof key === 'number') {
    return key
  }
  return stringify(key)
}
