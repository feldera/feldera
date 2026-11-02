import { untrack } from 'svelte'
import {
  ProgressBarAnimator,
  type ProgressFrame,
  type ProgressSample,
  zeroFrame
} from './progressBarAnimation'

/**
 * Animates a progress bar. `getSample` is reactive, and the poll changes its
 * value once in each `periodMs`. Refer to `progressBarAnimation.ts` for the
 * rules of the animation.
 *
 * `current` is the frame to render. Render the bar and all labels near it
 * from `current` only. Thus, they all change at the same time.
 *
 * Call this function when a component initializes, or in `$effect.root`.
 * When that owner is destroyed, this function cancels the timers that did not
 * run yet.
 */
export const useProgressBarAnimation = <T>(
  getSample: () => ProgressSample<T>,
  periodMs: number
): { readonly current: ProgressFrame<T> } => {
  // The bar starts empty. Then the first update grows it from 0.
  let frame = $state(untrack(() => zeroFrame(getSample())))
  const animator = new ProgressBarAnimator<T>(periodMs, (next) => (frame = next))

  $effect(() => animator.update(getSample()))
  $effect(() => () => animator.dispose())

  return {
    get current() {
      return frame
    }
  }
}
