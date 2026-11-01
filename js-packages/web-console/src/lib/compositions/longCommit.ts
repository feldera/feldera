/**
 * Finds the commits that take a long time. The view shows a progress bar only
 * for these commits. A short commit ends before the bar can show useful data.
 */

/** A commit is long if the estimate of its duration is more than this time. */
export const LONG_COMMIT_MS = 6000

/** The data of one poll that the detector uses. */
export type CommitPoll = {
  transactionId: number
  /** The commit progress in percent. Is `null` when no commit runs. */
  percent: number | null
}

/**
 * Estimates the full duration of a commit from its progress in two polls,
 * `periodMs` apart. Is `Infinity` if the progress did not increase.
 */
export const estimateCommitMs = (fromPercent: number, toPercent: number, periodMs: number) => {
  const gain = toPercent - fromPercent
  return gain > 0 ? (100 / gain) * periodMs : Infinity
}

export class LongCommitDetector {
  private previous: CommitPoll | undefined
  /** The transaction whose commit is long. It stays long until the commit ends. */
  private longTransactionId: number | undefined

  constructor(
    private readonly periodMs: number,
    private readonly thresholdMs = LONG_COMMIT_MS
  ) {}

  /**
   * Receives the next poll. Returns `true` while the commit of this poll is
   * long. A commit becomes long only if two polls in sequence show the same
   * transaction and the estimate is more than the threshold. Then it stays
   * long until it ends, even if it becomes faster.
   */
  update(poll: CommitPoll): boolean {
    const previous = this.previous
    this.previous = poll
    if (poll.percent === null) {
      this.longTransactionId = undefined
      return false
    }
    if (poll.transactionId === this.longTransactionId) {
      return true
    }
    this.longTransactionId = undefined
    if (
      previous?.percent == null ||
      previous.transactionId !== poll.transactionId ||
      estimateCommitMs(previous.percent, poll.percent, this.periodMs) <= this.thresholdMs
    ) {
      return false
    }
    this.longTransactionId = poll.transactionId
    return true
  }
}
