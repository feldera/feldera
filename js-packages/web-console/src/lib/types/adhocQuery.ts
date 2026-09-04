import type { SQLValueJS } from '$lib/types/sql'

/** Ad-hoc query result: the result's rows, plus - each in place of a row - the
 * message from a failed query and the notice that only the first rows were kept. */
export type Row = { cells: SQLValueJS[] } | { error: string } | { warning: string }

/** One row of the result. */
export const isDataRow = (row: Row): row is { cells: SQLValueJS[] } => 'cells' in row

/** The message from a query that failed. */
export const isErrorRow = (row: Row): row is { error: string } => 'error' in row

/** The notice that the result was cut at the row cap. */
export const isWarningRow = (row: Row): row is { warning: string } => 'warning' in row
