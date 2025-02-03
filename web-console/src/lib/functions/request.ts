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
    >,
    errorMsg?: string
  ) =>
  (...a: Args) =>
    x(...a).then(
      (res) =>
        res.data
          ? res.data
          : (() => {
              throw res.error
            })(),
      errorMsg
        ? () => {
            throw new Error(errorMsg)
          }
        : undefined
    )
