/**
 * Register a keyboard shortcut on the window while a scope is active.
 *
 * App-level shortcuts (Ctrl/Cmd-F to open search, and the like) are a scope concern, not a focus
 * concern: they must fire regardless of which element holds focus. A capture-phase window listener
 * delivers that reliably, where routing the key through a focused container is browser-fragile.
 *
 * Call during component initialization (it sets up an `$effect`). Pass `isActive` as a getter so
 * the listener attaches only while the scope is shown (e.g. the current tab) and detaches
 * otherwise; the effect re-runs whenever the values it reads change.
 *
 * @param matches   predicate identifying the shortcut (e.g. `isFindShortcut`)
 * @param handler   runs when the shortcut fires; the default is already prevented
 * @param isActive  whether the shortcut is currently in scope (default: always)
 */
export function useShortcut(
  matches: (e: KeyboardEvent) => boolean,
  handler: (e: KeyboardEvent) => void,
  isActive: () => boolean = () => true
): void {
  $effect(() => {
    if (!isActive()) {
      return
    }
    const onKeydown = (e: KeyboardEvent) => {
      if (matches(e)) {
        e.preventDefault()
        handler(e)
      }
    }
    window.addEventListener('keydown', onKeydown, { capture: true })
    return () => window.removeEventListener('keydown', onKeydown, { capture: true })
  })
}
