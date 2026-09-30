/**
 * The colour family of a pipeline status. Each tone maps to a pair of
 * `--color-status-<tone>` theme colours defined in `feldera-theme/feldera-modern.css`.
 *
 * - `neutral`: idle or shutting down
 * - `blue`: busy with a transition (starting, pausing, bootstrapping)
 * - `jade`: running and processing data
 * - `warning`: compiling, or needs the user's attention
 * - `error`: failed
 */
export type StatusTone = 'neutral' | 'blue' | 'jade' | 'warning' | 'error'

export interface StatusColors {
  chip: string
  dot: string
}

/**
 * The shape and type of every status chip: the pipeline status, the transaction
 * status and the commit progress chips. Combine it with a tone's `chip` colours.
 */
export const statusChipClass =
  'inline-flex items-center rounded-[3px] px-1.5 py-0.5 text-[12px] leading-4 font-medium tracking-[0.04px] whitespace-nowrap'

/**
 * The shape of a numeric badge next to a tab label or filter, such as an error
 * count. Combine it with a tone's `chip` colours.
 */
export const statusCounterClass = 'inline-block min-w-6 rounded-[3px] px-1 text-center font-medium'

// Class names are spelled out in full so that Tailwind can find them in the source.
export const statusToneColors: Record<StatusTone, StatusColors> = {
  neutral: {
    chip: 'bg-status-neutral-subtle text-status-neutral',
    dot: 'bg-status-neutral-dot'
  },
  blue: { chip: 'bg-status-blue-subtle text-status-blue', dot: 'bg-status-blue-dot' },
  jade: { chip: 'bg-status-jade-subtle text-status-jade', dot: 'bg-status-jade-dot' },
  warning: {
    chip: 'bg-status-warning-subtle text-status-warning',
    dot: 'bg-status-warning-dot'
  },
  error: { chip: 'bg-status-error-subtle text-status-error', dot: 'bg-status-error' }
}
