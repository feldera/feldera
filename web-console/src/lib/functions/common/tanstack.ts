type FunctionType = (...args: any) => any
type Arguments<F extends FunctionType> = F extends (...args: infer A) => any ? A : never

type ResType<U extends Record<string, FunctionType>, P extends keyof U> = {
  queryKey: readonly unknown[]
  queryFn: () => ReturnType<U[P]>
}

export const mkQuery = <U extends Record<string, FunctionType>>(
  source: U
): { [P in keyof U]: (...args: Arguments<U[P]>) => ResType<U, P> } =>
  Object.fromEntries(
    Object.entries(source).map(([key, value]) => {
      return [
        key,
        (...args: unknown[]) => {
          return {
            queryKey: [key, ...args], // key + '/' + args.map(String).join('/'),
            queryFn: () => value(...args)
          }
        }
      ]
    })
  ) as any
