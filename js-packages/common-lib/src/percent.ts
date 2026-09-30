/** A percentage string such as `30%`. */
export type Percent = `${number}%`

/**
 * Build a {@link Percent} from a number.
 * @example percent(30) // '30%'
 * @param value Percentage magnitude, e.g. `30` for `30%`.
 */
export const percent = (value: number): Percent => `${value}%`

/**
 * Read the numeric magnitude of a {@link Percent}.
 * @example percentValue('30%') // 30
 */
export const percentValue = (value: Percent): number => parseFloat(value)
