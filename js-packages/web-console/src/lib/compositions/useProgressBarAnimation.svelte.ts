import { untrack } from 'svelte'
import {
  ProgressBarAnimator,
  type ProgressBarOptions,
  type ProgressFrame,
  type ProgressSample,
  zeroFrame
} from './progressBarAnimation'

/**
 * Animates a progress bar. It reads `getSample` reactively. The poll gives a
 * new sample one time in each `periodMs`. Refer to `ProgressBarAnimator` for
 * the rules of the animation.
 *
 * `current` is the frame to render. Render the bar and all labels near it
 * from `current` only. Thus, they all change at the same time.
 *
 * Call this function when a component initializes, or in `$effect.root`.
 * When that owner is destroyed, the timers that did not run are cancelled.
 */
export const useProgressBarAnimation = <T>(
  getSample: () => ProgressSample<T>,
  periodMs: number,
  options: ProgressBarOptions = {}
): { readonly current: ProgressFrame<T> } => {
  // The bar starts empty. Then the first update grows it from 0.
  let frame = $state(untrack(() => zeroFrame(getSample())))
  const animator = new ProgressBarAnimator<T>(periodMs, (next) => (frame = next), options)

  $effect(() => animator.update(getSample()))
  $effect(() => () => animator.dispose())

  return {
    get current() {
      return frame
    }
  }
}
