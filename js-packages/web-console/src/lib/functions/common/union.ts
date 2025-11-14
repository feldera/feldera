export type NamesInUnion<T> = T extends string ? T : T extends T ? keyof T : never

export const unionName = <T extends string | Record<string, unknown>>(status: T) =>
  (typeof status === 'string' ? status : Object.keys(status)[0]) as NamesInUnion<T>
