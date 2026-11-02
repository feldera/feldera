import { untrack } from 'svelte'
import { Tween } from 'svelte/motion'

/**
 * Moves a shown number to each new value of `getSample`, linearly in
 * `durationMs`. A new value that comes before the move ends continues from the
 * shown number. When `key` changes, the number jumps to the new value instead.
 *
 * `current` is the number to show. It can be fractional while it moves.
 *
 * Call this function when a component initializes, or in `$effect.root`.
 */
export const useInterpolatedValue = (
  getSample: () => { value: number; key: unknown },
  durationMs: number
): { readonly current: number } => {
  const initial = untrack(getSample)
  const tween = new Tween(initial.value, { duration: durationMs })
  let previousKey = initial.key

  // A pre-effect makes the number jump in the same render as the change of the key.
  $effect.pre(() => {
    const { value, key } = getSample()
    const isSameKey = key === previousKey
    previousKey = key
    if (isSameKey && value === untrack(() => tween.target)) {
      // The tween already moves to this value, or shows it. A new `set` would restart the move.
      return
    }
    void tween.set(value, isSameKey ? undefined : { duration: 0 })
  })

  return tween
}
