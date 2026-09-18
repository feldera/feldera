/**
 * The durations this formats reach ten minutes and more: a cluster monitor event is written
 * at most every ten minutes, and keeps ageing once the monitor dies. Every component has to
 * roll over, or the label counts raw seconds.
 */
import Dayjs from 'dayjs'
import duration from 'dayjs/plugin/duration'
import { describe, expect, it } from 'vitest'
import { formatDuration } from './useElapsedTime'

// The app extends Dayjs once at startup; a unit test loads the plugin itself.
Dayjs.extend(duration)

const seconds = (n: number) => n * 1000
const minutes = (n: number) => seconds(60 * n)
const hours = (n: number) => minutes(60 * n)
const days = (n: number) => hours(24 * n)

describe('formatDuration', () => {
  it('rolls seconds over into minutes, hours and days', () => {
    expect(formatDuration(seconds(50))).toBe(' 50s')
    expect(formatDuration(minutes(9) + seconds(50))).toBe(' 9m 50s')
    expect(formatDuration(hours(1) + minutes(30))).toBe(' 1h 30m')
    expect(formatDuration(days(2) + hours(3) + minutes(4) + seconds(5))).toBe(' 2d 3h 4m 5s')
  })

  it('leaves out the components that are zero', () => {
    expect(formatDuration(hours(1))).toBe(' 1h')
    expect(formatDuration(days(1) + seconds(1))).toBe(' 1d 1s')
  })

  it('reports less than a second as nothing, so a caller can say "just now"', () => {
    expect(formatDuration(0)).toBe('')
    expect(formatDuration(999)).toBe('')
  })

  describe('precision dhm', () => {
    it('stops at minutes', () => {
      expect(formatDuration(minutes(9) + seconds(50), 'dhm')).toBe(' 9m')
      expect(formatDuration(hours(1) + minutes(30) + seconds(30), 'dhm')).toBe(' 1h 30m')
    })

    it('reports anything shorter than a minute as such', () => {
      expect(formatDuration(0, 'dhm')).toBe('< 1m')
      expect(formatDuration(seconds(59), 'dhm')).toBe('< 1m')
    })

    // One minute exactly used to satisfy both branches and print ' 1m< 1m'.
    it('reports one minute exactly as a minute', () => {
      expect(formatDuration(minutes(1), 'dhm')).toBe(' 1m')
    })
  })
})
