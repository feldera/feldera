import { describe, expect, it } from 'vitest'
import {
    BooleanValue,
    BytesValue,
    CountValue,
    MissingValue,
    PercentValue,
    PropertyValue,
    StringValue,
    TimeValue
} from './profile.js'

describe('CountValue', () => {
    it('toString formats whole numbers with K/M/B suffixes', () => {
        expect(new CountValue(0).toString()).toBe('0')
        expect(new CountValue(42).toString()).toBe('42')
        expect(new CountValue(999).toString()).toBe('999')
        expect(new CountValue(1_500).toString()).toBe('1.5K')
        expect(new CountValue(1_500_000).toString()).toBe('1.5M')
        expect(new CountValue(2_345_000_000).toString()).toBe('2.35B')
    })

    it('toString preserves precision for sub-unit values', () => {
        // Regression: maxFractionDigits=2 rounded 0.004 down to "0"; significant digits keeps it visible.
        expect(new CountValue(0.004).toString()).toBe('0.004')
        expect(new CountValue(0.5).toString()).toBe('0.5')
    })

    it('average is arithmetic mean over CountValues', () => {
        const avg = new CountValue(10).average([new CountValue(20), new CountValue(30)])
        expect(avg).toBeInstanceOf(CountValue)
        expect(avg.getNumericValue().unwrap()).toBe(20)
        expect(avg.toString()).toBe('20')
    })

    it('average skips MissingValue neighbours', () => {
        const avg = new CountValue(10).average([MissingValue.INSTANCE, new CountValue(30)])
        expect(avg.getNumericValue().unwrap()).toBe(20)
    })
})

describe('BytesValue', () => {
    it('toString scales by 1024 with B/KiB/MiB suffixes', () => {
        expect(new BytesValue(0).toString()).toBe('0B')
        expect(new BytesValue(512).toString()).toBe('512B')
        expect(new BytesValue(2048).toString()).toBe('2KiB')
        expect(new BytesValue(1024 * 1024 * 3).toString()).toBe('3MiB')
    })

    it('toString preserves precision for sub-byte values', () => {
        // Edge case: rate-like metrics can produce fractional bytes; they should not round to "0B".
        expect(new BytesValue(0.4).toString()).toBe('0.4B')
        expect(new BytesValue(0.004).toString()).toBe('0.004B')
    })

    it('average is arithmetic mean over BytesValues', () => {
        const avg = new BytesValue(1024).average([new BytesValue(2048), new BytesValue(3072)])
        expect(avg).toBeInstanceOf(BytesValue)
        expect(avg.getNumericValue().unwrap()).toBe(2048)
        expect(avg.toString()).toBe('2KiB')
    })
})

describe('TimeValue', () => {
    it('toString picks unit by magnitude', () => {
        expect(new TimeValue(0).toString()).toBe('0s')
        expect(new TimeValue(0.5).toString()).toBe('500ms')
        expect(new TimeValue(2.5).toString()).toBe('2.5s')
    })

    it('toString preserves precision for sub-microsecond values', () => {
        // Regression for the original bug: 4ns -> "0us" with maxFractionDigits=2.
        // Significant digits keeps the displayed value non-zero.
        expect(new TimeValue(4e-9).toString()).toBe('0.004us')
        expect(new TimeValue(9.9e-10).toString()).toBe('0.00099us')
        expect(new TimeValue(1e-9).toString()).toBe('0.001us')
        // At and above 1us, fall back to fraction digits (no precision loss).
        expect(new TimeValue(5e-6).toString()).toBe('5us')
    })

    it('toString renders >=1h as [days ]HH:MM:SS', () => {
        // 1h 2m 3s
        expect(new TimeValue(3723).toString()).toBe('01:02:03')
        // 1 day, 2h 3m 4s
        expect(new TimeValue(86400 + 2 * 3600 + 3 * 60 + 4).toString()).toBe('1day 02:03:04')
        // 3 days
        expect(new TimeValue(3 * 86400).toString()).toBe('3days 00:00:00')
    })

    it('average is arithmetic mean of seconds', () => {
        const avg = new TimeValue(0.5).average([new TimeValue(1.0), new TimeValue(1.5)])
        expect(avg).toBeInstanceOf(TimeValue)
        expect(avg.getNumericValue().unwrap()).toBeCloseTo(1.0, 9)
        expect(avg.toString()).toBe('1s')
    })
})

describe('PercentValue', () => {
    it('toString uses 1-decimal form for typical percents', () => {
        expect(new PercentValue(50, 100).toString()).toBe('50.0%')
        expect(new PercentValue(0, 100).toString()).toBe('0.0%')
        expect(new PercentValue(33, 100).toString()).toBe('33.0%')
    })

    it('toString preserves precision below 0.1%', () => {
        // Regression: toFixed(1) rounded 0.04% to "0.0%". Significant digits keeps it visible.
        expect(new PercentValue(4, 10_000).toString()).toBe('0.04%')
        expect(new PercentValue(34, 100_000).toString()).toBe('0.034%')
    })

    it('toString returns N/A when numerator and denominator both make it undefined', () => {
        // PercentValue's getNumericValue treats denom=0 as 0%, not None — verify display path.
        expect(new PercentValue(5, 0).toString()).toBe('0.0%')
    })

    it('average is arithmetic mean of per-worker percents, not weighted', () => {
        // Bloom-filter-style example from the design discussion:
        //   worker 0: 5/10 = 50%
        //   worker 1: 0/1000 = 0%
        // Arithmetic mean = 25%, NOT weighted (5/1010 ≈ 0.495%).
        const avg = new PercentValue(5, 10).average([new PercentValue(0, 1000)])
        expect(avg).toBeInstanceOf(PercentValue)
        expect(avg.getNumericValue().unwrap()).toBeCloseTo(25, 6)
        expect(avg.toString()).toBe('25.0%')
    })

    it('average over three equal-denom workers', () => {
        const avg = new PercentValue(10, 100).average([
            new PercentValue(20, 100),
            new PercentValue(30, 100)
        ])
        expect(avg.getNumericValue().unwrap()).toBeCloseTo(20, 6)
    })
})

describe('MissingValue', () => {
    it('toString reports N/A', () => {
        expect(MissingValue.INSTANCE.toString()).toBe('N/A')
    })

    it('average delegates to the first non-missing neighbour', () => {
        // First non-missing in others is a CountValue → average should be over the counts.
        const avg = MissingValue.INSTANCE.average([new CountValue(10), new CountValue(20)])
        expect(avg).toBeInstanceOf(CountValue)
        expect(avg.getNumericValue().unwrap()).toBe(15)
    })

    it('average stays missing when every neighbour is missing', () => {
        const avg = MissingValue.INSTANCE.average([MissingValue.INSTANCE, MissingValue.INSTANCE])
        expect(avg).toBeInstanceOf(MissingValue)
    })
})

describe('StringValue / BooleanValue', () => {
    it('StringValue.toString is the raw string', () => {
        expect(new StringValue('persistent-id-foo').toString()).toBe('persistent-id-foo')
    })

    it('BooleanValue.toString is the literal "true"/"false"', () => {
        expect(new BooleanValue(true).toString()).toBe('true')
        expect(new BooleanValue(false).toString()).toBe('false')
    })

    it('StringValue.average is missing', () => {
        expect(new StringValue('x').average([new StringValue('y')])).toBeInstanceOf(MissingValue)
    })

    it('BooleanValue.average is missing', () => {
        expect(new BooleanValue(true).average([new BooleanValue(false)])).toBeInstanceOf(MissingValue)
    })
})

// Regression guard: a non-zero reading should not be
// rendered as plain "0" (or "0<unit>") just because the magnitude is small. The rule per
// kind:
//   - Count/Bytes/Time/Percent: the displayed numeric portion must not be exactly "0".
//   - Bytes/Time/Percent: the unit suffix (B/KiB/..., us/ms/s, %) must still be present.
// Counts have no inherent unit, so only the "not plain 0" rule applies to them.
describe('near-zero values keep precision and unit text', () => {
    // Helper: assert that `displayed` is not a bare-zero rendering and still ends with `unit`.
    const assertNonZeroWithUnit = (displayed: string, unit: string) => {
        expect(displayed.endsWith(unit)).toBe(true)
        const numericPart = displayed.slice(0, displayed.length - unit.length).trim()
        // Reject "0", "0.0", "0.00", "-0", etc. — any pure-zero rendering of the number.
        expect(numericPart).not.toMatch(/^-?0(?:\.0+)?$/)
    }

    it('CountValue near-zero values do not collapse to "0"', () => {
        for (const v of [1e-3, 1e-6, 1e-9, 4e-9, 4.2e-4]) {
            const s = new CountValue(v).toString()
            expect(s, `CountValue(${v})`).not.toMatch(/^-?0(?:\.0+)?$/)
        }
    })

    it('BytesValue near-zero values keep the B suffix and a non-zero numeric part', () => {
        for (const v of [0.5, 0.04, 0.004, 1e-6]) {
            assertNonZeroWithUnit(new BytesValue(v).toString(), 'B')
        }
    })

    it('TimeValue sub-microsecond values keep the us suffix and a non-zero numeric part', () => {
        // Below 0.001 s (the original bug's regime): every value below 1 us would have rounded
        // to "0us" under the old formatter.
        for (const v of [1e-9, 4e-9, 5e-10, 9.9e-10]) {
            assertNonZeroWithUnit(new TimeValue(v).toString(), 'us')
        }
    })

    it('TimeValue near-zero ms values keep the ms suffix and a non-zero numeric part', () => {
        // Above 1 us so they hit the ms branch; still small enough to be at risk of rounding.
        for (const v of [0.002, 0.0015]) {
            assertNonZeroWithUnit(new TimeValue(v).toString(), 'ms')
        }
    })

    it('PercentValue values below 0.1% keep the % suffix and a non-zero numeric part', () => {
        // toFixed(1) used to round these to "0.0%".
        const tinyPercents: Array<[number, number]> = [
            [4, 10_000], // 0.04%
            [34, 100_000], // 0.034%
            [1, 1_000] // 0.1% boundary — must still print non-zero
        ]
        for (const [num, denom] of tinyPercents) {
            assertNonZeroWithUnit(new PercentValue(num, denom).toString(), '%')
        }
    })

    it('exact zero still renders as the kind-appropriate zero (sanity check on the guard)', () => {
        // The "not plain 0" rule applies only to NON-zero readings — exact zero must keep
        // working. This guards against an overzealous future fix.
        expect(new CountValue(0).toString()).toBe('0')
        expect(new BytesValue(0).toString()).toBe('0B')
        expect(new TimeValue(0).toString()).toBe('0s')
        expect(new PercentValue(0, 100).toString()).toBe('0.0%')
    })
})

describe('PropertyValue contract', () => {
    it('every concrete subclass implements average and toString without throwing', () => {
        const samples: PropertyValue[] = [
            new CountValue(1),
            new BytesValue(1),
            new TimeValue(1),
            new PercentValue(50, 100),
            new StringValue('x'),
            new BooleanValue(true),
            MissingValue.INSTANCE
        ]
        for (const v of samples) {
            expect(typeof v.toString()).toBe('string')
            // Self-average: every kind should accept an empty `others` array without throwing.
            expect(() => v.average([])).not.toThrow()
        }
    })
})
