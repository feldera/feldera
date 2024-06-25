import JSONbig from 'true-json-bigint'

export const handled =
  <Args extends any[], Data, Error>(
    x: (...a: Args) => Promise<
      | {
          data: Data
          error: undefined
        }
      | {
          data: undefined
          error: Error
        }
    >
  ) =>
  (...a: Args) =>
    x(...a).then((res) =>
      res.data
        ? res.data
        : (() => {
            throw new Error(JSONbig.stringify(res.error))
          })()
    )
