import type { PipelineAction } from '$lib/services/pipelineManager'

type Cb = () => Promise<void>

const callbacks: Record<string, Partial<Record<PipelineAction, Cb[]>>> = $state({})

export function usePipelineActionCallbacks() {
  const pop = (pipelineName: string, action: PipelineAction) => {
    callbacks[pipelineName] ??= {}
    callbacks[pipelineName][action] ??= []
    return callbacks[pipelineName][action].pop()
  }
  return {
    add(pipelineName: string, action: PipelineAction, callback: Cb) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      callbacks[pipelineName][action].push(callback)
    },
    remove(pipelineName: string, action: PipelineAction, callback: Cb) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      const idx = callbacks[pipelineName][action].findIndex((cb) => cb === callback)
      if (idx === -1) {
        return
      }
      callbacks[pipelineName][action].splice(idx, 1)
    },
    pop,
    popIterator: (pipelineName: string, action: PipelineAction) => ({
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
    popAll(pipelineName: string, action: PipelineAction) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      return callbacks[pipelineName][action]?.splice(0)
    },
    getAll(pipelineName: string, action: PipelineAction) {
      return callbacks[pipelineName]?.[action] ?? []
    }
  }
}
