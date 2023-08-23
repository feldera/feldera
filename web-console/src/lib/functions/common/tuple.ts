export function tuple<T extends unknown[]>(...t: [...T]) {
  return t
}

export function tuples<T extends unknown[]>(t: [...T][]) {
  return t
}
