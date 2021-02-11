// discard message if bad value - not JSON object
export function validateValue(value) {
  if (value == null || typeof value !== 'object') {
    throw new Error(`value should be an object, was ${typeof value}`)
  }
}
