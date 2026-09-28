import { untrack } from 'svelte'
import {
  type AnimationScheduler,
  browserScheduler,
  followFrame,
  ProgressBarAnimator,
  type ProgressFrame,
  type ProgressSample
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
  scheduler: AnimationScheduler = browserScheduler
): { readonly current: ProgressFrame<T> } => {
  let frame = $state(untrack(() => followFrame(getSample(), periodMs)))
  const animator = new ProgressBarAnimator<T>(periodMs, (next) => (frame = next), scheduler)

  $effect(() => animator.update(getSample()))
  $effect(() => () => animator.dispose())

  return {
    get current() {
      return frame
    }
  }
}
