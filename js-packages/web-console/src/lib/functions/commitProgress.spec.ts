import { describe, expect, it } from 'vitest'
import { commitProgressPercents, operatorTotal } from './commitProgress'

const progress = (
  completed: number,
  in_progress: number,
  remaining: number,
  processed = 0,
  total = 0
) => ({
  completed,
  in_progress,
  remaining,
  in_progress_processed_records: processed,
  in_progress_total_records: total
})

describe('commitProgressPercents', () => {
  it('has no combined percent without counts', () => {
    expect(commitProgressPercents(null)).toEqual({ combined: null, completed: 0 })
    expect(commitProgressPercents(undefined)).toEqual({ combined: null, completed: 0 })
    expect(commitProgressPercents(progress(0, 0, 0))).toEqual({ combined: null, completed: 0 })
  })

  it('adds the processed part of the in-progress operators to the combined percent', () => {
    // 2 completed plus half of 2 in progress, out of 8.
    expect(commitProgressPercents(progress(2, 2, 4, 50, 100))).toEqual({
      combined: 37.5,
      completed: 25
    })
  })

  it('counts no in-progress part when the record total is not known', () => {
    expect(commitProgressPercents(progress(2, 2, 4))).toEqual({ combined: 25, completed: 25 })
  })

  it('totals all the operators', () => {
    expect(operatorTotal(progress(1, 2, 3))).toBe(6)
    expect(operatorTotal(null)).toBe(0)
  })
})
