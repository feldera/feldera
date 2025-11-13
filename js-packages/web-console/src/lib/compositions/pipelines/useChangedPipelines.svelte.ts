// let state = $state(new Set<string>())

// export const useChangedPipelines = () => {
//   return {
//     add (pipelineName: string) { state.add(pipelineName) },
//     remove (pipelineName: string) { state.delete(pipelineName) },
//     rename (oldPipelineName: string, newPipelineName: string) { state.delete(oldPipelineName); state.add(newPipelineName) },
//     get list () { return state as { has: (pipelineName: string) => boolean } },
//     has (pipelineName: string) { return state.has(pipelineName) }
//   }
// }

let state = $state([] as string[])

export const useChangedPipelines = () => {
  return {
    add(pipelineName: string) {
      if (!state.includes(pipelineName)) state.push(pipelineName)
    },
    remove(pipelineName: string) {
      const idx = state.findIndex((x) => x === pipelineName)
      if (idx !== -1) state.splice(idx, 1)
    },
    rename(oldPipelineName: string, newPipelineName: string) {
      const idx = state.findIndex((x) => x === oldPipelineName)
      if (idx !== -1) state.splice(idx, 1, newPipelineName)
    },
    get list() {
      return state
    },
    has(pipelineName: string) {
      return state.includes(pipelineName)
    }
  }
}
