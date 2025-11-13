import type { PipelineAction } from '$lib/services/pipelineManager'

type Cb = (pipelineName: string) => Promise<void>

type Action = PipelineAction | 'delete'

const callbacks: Record<string, Partial<Record<Action, Cb[]>>> = $state({})

const pop = (pipelineName: string, action: Action) => {
  callbacks[pipelineName] ??= {}
  callbacks[pipelineName][action] ??= []
  return callbacks[pipelineName][action].pop()
}

export function usePipelineActionCallbacks() {
  return {
    add(pipelineName: string, action: Action, callback: Cb) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      callbacks[pipelineName][action].push(callback)
    },
    remove(pipelineName: string, action: Action, callback: Cb) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      const idx = callbacks[pipelineName][action].findIndex((cb) => cb === callback)
      if (idx === -1) {
        return
      }
      callbacks[pipelineName][action].splice(idx, 1)
    },
    pop,
    popIterator: (pipelineName: string, action: Action) => ({
      [Symbol.iterator](): Iterator<Cb> {
        return {
          next: (): IteratorResult<Cb> => {
            const value = pop(pipelineName, action)
            if (value !== undefined) {
              return { value, done: false }
            } else {
              return { value: undefined as any, done: true }
            }
          }
        }
      }
    }),
    popAll(pipelineName: string, action: Action) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      return callbacks[pipelineName][action]?.splice(0)
    },
    getAll(pipelineName: string, action: Action) {
      return callbacks[pipelineName]?.[action] ?? []
    }
  }
}
