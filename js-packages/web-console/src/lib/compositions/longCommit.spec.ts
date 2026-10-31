import { describe, expect, it } from 'vitest'
import { estimateCommitMs, LONG_COMMIT_MS, LongCommitDetector } from './longCommit'

const PERIOD = 2000

const committing = (transactionId: number, percent: number) => ({ transactionId, percent })
const notCommitting = (transactionId: number) => ({ transactionId, percent: null })

/** Returns the result of each poll. */
const run = (polls: { transactionId: number; percent: number | null }[]) => {
  const detector = new LongCommitDetector(PERIOD)
  return polls.map((poll) => detector.update(poll))
}

describe('estimateCommitMs', () => {
  it('extrapolates the gain between two polls to the full commit', () => {
    // 10% in 2 s gives 100% in 20 s.
    expect(estimateCommitMs(20, 30, PERIOD)).toBe(20000)
  })

  it('is infinite when the progress does not increase', () => {
    expect(estimateCommitMs(30, 30, PERIOD)).toBe(Infinity)
    expect(estimateCommitMs(30, 10, PERIOD)).toBe(Infinity)
  })
})

describe('LongCommitDetector', () => {
  it('is not long after one poll only', () => {
    expect(run([committing(1, 0)])).toEqual([false])
  })

  it('is long when two polls of one commit give an estimate above the threshold', () => {
    // 20% in one period gives 5 periods, which is 10 s.
    expect(run([committing(1, 0), committing(1, 20)])).toEqual([false, true])
  })

  it('is not long when the estimate is at the threshold or below', () => {
    const detector = new LongCommitDetector(PERIOD, 4000)
    // 50% in one period gives 2 periods, which is 4 s.
    expect([committing(1, 0), committing(1, 50)].map((p) => detector.update(p))).toEqual([
      false,
      false
    ])
    expect(run([committing(1, 0), committing(1, 90)])).toEqual([false, false])
  })

  it('is long when the commit does not progress between two polls', () => {
    expect(run([committing(1, 40), committing(1, 40)])).toEqual([false, true])
  })

  it('does not compare the polls of two different transactions', () => {
    expect(run([committing(1, 50), committing(2, 50)])).toEqual([false, false])
  })

  it('does not compare with a poll that has no commit progress', () => {
    expect(run([notCommitting(1), committing(1, 10), committing(1, 11)])).toEqual([
      false,
      false,
      true
    ])
  })

  it('stays long until the commit ends, also when the commit becomes fast', () => {
    expect(
      run([
        committing(1, 0),
        committing(1, 5),
        committing(1, 95),
        notCommitting(1),
        committing(1, 96)
      ])
    ).toEqual([false, true, true, false, false])
  })

  it('stops being long when a different transaction replaces the commit', () => {
    expect(run([committing(1, 0), committing(1, 5), committing(2, 5)])).toEqual([
      false,
      true,
      false
    ])
  })

  it('is never long without two polls of one commit in sequence', () => {
    // All sequences of four polls from a small set of polls.
    const choices = [notCommitting(1), committing(1, 0), committing(1, 50), committing(2, 50)]
    const sequences = (length: number): (typeof choices)[] =>
      length === 0 ? [[]] : sequences(length - 1).flatMap((s) => choices.map((c) => [...s, c]))
    for (const polls of sequences(4)) {
      run(polls).forEach((isLong, i) => {
        if (isLong) {
          expect(polls[i].percent).not.toBeNull()
          const since = polls.slice(0, i + 1).findLastIndex((p) => p.percent === null)
          const commit = polls.slice(since + 1, i + 1)
          // The commit had two polls of the same transaction in sequence.
          expect(
            commit.some((p, j) => j > 0 && p.transactionId === commit[j - 1].transactionId)
          ).toBe(true)
        }
      })
    }
  })
})
