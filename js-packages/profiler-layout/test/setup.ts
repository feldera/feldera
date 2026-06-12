// Monaco's internal `WordHighlighter` and friends queue async work that they cancel by
// rejecting a `Canceled` promise when disposed. The rejection is intentional and harmless,
// but it surfaces as an unhandled rejection during test teardown and fails the run.
// Swallow only that exact shape; let everything else propagate.

const isMonacoCanceled = (reason: unknown): boolean =>
  reason instanceof Error && reason.name === 'Canceled'

window.addEventListener('unhandledrejection', (event) => {
  if (isMonacoCanceled(event.reason)) {
    event.preventDefault()
  }
})
