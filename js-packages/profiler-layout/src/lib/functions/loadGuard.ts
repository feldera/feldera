/**
 * Runs async loading work behind a guard that toggles `isLoading` and routes
 * any thrown error to a caller-provided handler — an inline try/catch/finally
 * lifted out of every call site.
 *
 * Example:
 *   const withLoadGuard = createLoadGuard({
 *     setLoading: (loading) => { isLoading = loading }
 *   })
 *
 *   async function loadProfile(timestamp: Date) {
 *     const processed = await processProfileFiles(...)
 *     selectedProfile = timestamp
 *     profileData = ...
 *   }
 *
 *   async function handleSelectTimestamp(timestamp: Date) {
 *     errorMessage = ''
 *     await withLoadGuard(
 *       () => loadProfile(timestamp),
 *       (e) => {
 *         errorMessage = e instanceof Error && e.message
 *           ? e.message
 *           : 'Failed to load the profile snapshot.'
 *       }
 *     )
 *   }
 *
 * Only one load runs at a time per guard: calling the returned function while a
 * previous run hasn't finished returns immediately without doing anything.
 * That's enough race protection for UI flows where the trigger (button, file
 * picker, timestamp selector) is disabled while `isLoading` is true — and saves
 * having to thread an `isLatest()` check through every awaited step of the work.
 *
 * `onError` is passed per call so each call site can choose its own fallback
 * message, much like the catch block it replaces.
 */
export function createLoadGuard(opts: {
  setLoading: (loading: boolean) => void
  onFinally?: () => void
}) {
  let isRunning = false

  return async function run(
    work: () => Promise<void>,
    onError: (e: unknown) => void
  ): Promise<void> {
    if (isRunning) {
      return
    }
    isRunning = true
    opts.setLoading(true)
    try {
      await work()
    } catch (e) {
      onError(e)
    } finally {
      isRunning = false
      opts.setLoading(false)
      opts.onFinally?.()
    }
  }
}
