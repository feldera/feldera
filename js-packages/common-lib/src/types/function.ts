export type FunctionType = (...args: any) => any
export type Arguments<F extends FunctionType> = F extends (...args: infer A) => any ? A : never
