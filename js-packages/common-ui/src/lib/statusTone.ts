/**
 * The colour family of a pipeline status. Each tone maps to a set of
 * `--color-status-<tone>` theme colours defined in `feldera-theme/feldera-modern.css`.
 *
 * - `neutral`: idle or shutting down
 * - `blue`: busy with a transition (starting, pausing, bootstrapping)
 * - `jade`: running and processing data
 * - `warning`: compiling, or needs the user's attention
 * - `error`: failed
 */
export type StatusTone = 'neutral' | 'blue' | 'jade' | 'warning' | 'error'

/** The classes of a tone, one for each purpose. A dot and a chip of the same tone have
 *  different colors. */
export interface StatusColors {
  /** Background and text of a chip or a badge */
  chip: string
  /** Background of a status dot */
  dot: string
  /** Color of a status icon in the incident list of the health views */
  healthIcon: string
  /** Background of an event dot and a legend swatch in the health views */
  healthDot: string
  /** Color of a status label in the header of a status timeline */
  healthText: string
  /** SVG fill of a bar in a status timeline */
  healthBar: string
  /** SVG fill of a bar in a status timeline under the pointer or in the selection */
  healthBarHighlight: string
  /** Background of the cluster health dot in the profile menu, the same color as `healthIcon` */
  healthIndicator: string
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
    dot: 'bg-status-neutral-dot',
    healthIcon: 'text-surface-400-600',
    healthDot: 'bg-tertiary-100-900',
    healthText: 'text-tertiary-400-600',
    healthBar: 'fill-tertiary-100-900',
    healthBarHighlight: 'fill-tertiary-100-900',
    healthIndicator: 'bg-surface-400-600'
  },
  blue: {
    chip: 'bg-status-blue-subtle text-status-blue',
    dot: 'bg-status-blue-dot',
    healthIcon: 'text-blue-500',
    healthDot: 'bg-blue-500',
    healthText: 'text-blue-500',
    healthBar: 'fill-blue-500',
    healthBarHighlight: 'fill-blue-600',
    healthIndicator: 'bg-blue-500'
  },
  jade: {
    chip: 'bg-status-jade-subtle text-status-jade',
    dot: 'bg-status-jade-dot',
    healthIcon: 'text-success-500',
    healthDot: 'bg-green-500',
    healthText: 'text-green-500',
    healthBar: 'fill-green-500',
    healthBarHighlight: 'fill-green-600',
    healthIndicator: 'bg-success-500'
  },
  warning: {
    chip: 'bg-status-warning-subtle text-status-warning',
    dot: 'bg-status-warning-dot',
    healthIcon: 'text-warning-500',
    healthDot: 'bg-yellow-500',
    healthText: 'text-yellow-500',
    healthBar: 'fill-yellow-500',
    healthBarHighlight: 'fill-yellow-600',
    healthIndicator: 'bg-warning-500'
  },
  // The theme has no separate dot color for errors.
  error: {
    chip: 'bg-status-error-subtle text-status-error',
    dot: 'bg-status-error',
    healthIcon: 'text-error-500',
    healthDot: 'bg-red-500',
    healthText: 'text-red-500',
    healthBar: 'fill-red-500',
    healthBarHighlight: 'fill-red-600',
    healthIndicator: 'bg-error-500'
  }
}
