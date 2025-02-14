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
    x(...a).then(
      (res) =>
        res.data
          ? res.data
          : (() => {
              throw res.error
            })(),
      (e) => {
        // if (e instanceof TypeError && e.message === 'Failed to fetch') {
        //   return {
        //     pipelineName: pipeline_name,
        //     status: 'not running' as const
        //   }
        // }
        throw e
      }
    )
