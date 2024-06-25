import deepEq from 'fast-deep-equal'

export const reactiveEq = <T>(a: T, b: T) =>
  a && b && typeof a === 'object' ? deepEq(Object.entries(a), Object.entries(b)) : a === b
