export function dateMax(first: Date, ...rest: Date[]): Date {
  if (rest.length === 0) return first
  const [next, ...nextRest] = rest
  const maxOfRest = dateMax(next, ...nextRest)
  return first > maxOfRest ? first : maxOfRest
}

export function dateMin(first: Date, ...rest: Date[]): Date {
  if (rest.length === 0) return first
  const [next, ...nextRest] = rest
  const minOfRest = dateMin(next, ...nextRest)
  return first < minOfRest ? first : minOfRest
}

/**
 * Rounds a Date object up to the next whole hour (ceiling).
 * @param {Date} date The date to round up. Defaults to the current time if not provided.
 * @returns {Date} The ceiled date object.
 */
export function ceilToHour(date = new Date()) {
  const millisecondsInHour = 1000 * 60 * 60
  // Use Math.ceil to always round up the division result
  const ceiledTimestamp = Math.ceil(date.getTime() / millisecondsInHour) * millisecondsInHour
  return new Date(ceiledTimestamp)
}
