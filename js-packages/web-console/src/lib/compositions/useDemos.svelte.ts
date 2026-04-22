import { type Demo, getDemos } from '$lib/services/pipelineManager'

let demos = $state<(Demo | null)[]>(new Array<Demo | null>(9).fill(null))
let started = false

/**
 * Kick off the demos fetch once per page load. Subsequent calls are no-ops,
 * so home ↔ demos navigation reuses the same in-flight request. Failures
 * are swallowed (demos are non-critical content).
 */
export const loadDemos = () => {
  if (started) {
    return
  }
  started = true
  getDemos().then(
    (ds) => {
      demos = ds
    },
    (e) => {
      console.warn('Failed to load demos:', e)
      demos = []
    }
  )
}

export const useDemos = () => ({
  get current() {
    return demos
  }
})
