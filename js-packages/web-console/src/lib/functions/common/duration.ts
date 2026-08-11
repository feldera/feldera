import type { SameNullability } from '$lib/types/common/nullable'

declare const microsecondsBrand: unique symbol

/**
 * A duration in microseconds.
 */
export type Microseconds = number & { readonly [microsecondsBrand]: true }

/**
 * Marks `micros` as a duration in microseconds, passing a missing value through
 * unchanged.
 */
export const microseconds = <T extends number | null | undefined>(
  micros: T
): SameNullability<Microseconds, T> => micros as SameNullability<Microseconds, T>
