declare module '@observablehq/runtime' {
  export type Define = (runtime: any, observer: (name: string) => Inspector | undefined) => any
  export type VariableDefinition = (...args: any[]) => any

  export class Variable {
    define(definition): void
    define(name: string, definition): void
    define(inputs: string[], definition): void
    define(name, inputs: string[], definition): void
  }

  export class Runtime {
    constructor(builtins?: Record<string, any>, global?: any)
    module(): Module
    module(define: Define): Module
    module(observer: (name: string) => Inspector | undefined): Module
    module(define: Define, observer: (name: string) => Inspector | undefined): Module
    dispose(): void
  }

  export class Module {
    variable(observer?: (name: string) => Inspector | undefined): Variable
    derive(specifiers: { name: string; alias?: string }[], source: Module): Module
    define(definition: VariableDefinition): void
    define(name: string, definition: VariableDefinition): void
    define(inputs: string[], definition: VariableDefinition): void
    define(name: string, inputs: string[], definition: VariableDefinition): void
    import(name: string, from: Module): void
    import(name: string, alias: string, from: Module): void
    redefine(name: string, definition: VariableDefinition): void
    redefine(name: string, inputs: string[], definition: VariableDefinition): void
    value(name: string): any
  }
  export class Inspector {
    constructor(node: HTMLElement)
    pending()
    fulfilled(value: any)
    rejected(error: any)
    into(container: HTMLElement)
    into(selector: string)
  }
  namespace Inputs {
    export const range = (
      range: [min: number, max: number],
      props: { value: number; step?: number; label?: string }
    ) => any
  }
}
