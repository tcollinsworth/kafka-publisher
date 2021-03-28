// discard message if bad key - (null, undefined, Boolean, Symbol) must be (string, number, object)
export function validateKey(key) {
  if (typeof key === 'string' || typeof key === 'number') {
    return key
  }

  throw new Error(`key should be a number or string, was ${typeof key}`)
}
