import type { CommitProgressSummary } from '#lib/services/manager/index.ts'

/** The number of operators that the commit or the bootstrap flushes. */
export const operatorTotal = (progress: CommitProgressSummary | null | undefined) =>
  progress ? progress.completed + progress.in_progress + progress.remaining : 0

/**
 * Converts the operator counts to percents of the total.
 * - `completed`: the operators that are fully flushed.
 * - `combined`: `completed` plus the part of the in-progress operators that is
 *   flushed. Is `null` when there are no counts.
 */
export const commitProgressPercents = (
  progress: CommitProgressSummary | null | undefined
): { combined: number | null; completed: number } => {
  const total = operatorTotal(progress)
  if (!progress || total === 0) {
    return { combined: null, completed: 0 }
  }
  const inProgressFraction =
    progress.in_progress_total_records > 0
      ? progress.in_progress_processed_records / progress.in_progress_total_records
      : 0
  return {
    combined: ((progress.completed + progress.in_progress * inProgressFraction) / total) * 100,
    completed: (progress.completed / total) * 100
  }
}
