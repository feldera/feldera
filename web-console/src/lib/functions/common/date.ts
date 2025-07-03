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
