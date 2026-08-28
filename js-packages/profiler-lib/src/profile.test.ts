import { describe, expect, it } from 'vitest'
import {
    AggregationMode,
    BooleanValue,
    BytesValue,
    categoryShares,
    CircuitProfile,
    measurementCategory,
    ComplexNode,
    CountValue,
    Measurement,
    MissingValue,
    PercentValue,
    PropertyValue,
    RatioValue,
    SimpleNode,
    StringValue,
    TimeValue,
    totalShare,
    type JsonProfiles
} from './profile.js'
import type { Dataflow } from './dataflow.js'
import { NumericRange, Option } from './util.js'

describe('CircuitProfile.isTop', () => {
    it('recognises the toplevel node by the parsed root id', () => {
        const profile = new CircuitProfile(1, 'n')
        expect(profile.isTop('n')).toBe(true)
        expect(profile.isTop('n0')).toBe(false)
        expect(profile.isTop('42')).toBe(false)
    })

    it('does not assume the root id is "n"', () => {
        // The format conventionally emits "n", but isTop must follow whatever the profile carries.
        const profile = new CircuitProfile(1, 'root-7')
        expect(profile.isTop('root-7')).toBe(true)
        expect(profile.isTop('n')).toBe(false)
    })
})

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

    it('average is a weighted mean: sum(num) / sum(denom)', () => {
        //   worker 0: 5/10  = 50%
        //   worker 1: 0/1000 = 0%
        // Weighted = 5/1010 ≈ 0.495%, NOT the unweighted arithmetic mean (25%).
        const avg = new PercentValue(5, 10).average([new PercentValue(0, 1000)])
        expect(avg).toBeInstanceOf(PercentValue)
        expect(avg.getNumericValue().unwrap()).toBeCloseTo(100 * 5 / 1010, 6)
    })

    it('average over three equal-denom workers matches the per-worker mean', () => {
        // With equal denominators, the weighted mean collapses to the arithmetic mean.
        const avg = new PercentValue(10, 100).average([
            new PercentValue(20, 100),
            new PercentValue(30, 100)
        ])
        expect(avg.getNumericValue().unwrap()).toBeCloseTo(20, 6)
    })

    it('WeightedRatio pools the terms instead of adding the rates', () => {
        // Two nodes hitting a cache 50 times out of 100 lookups make a region that hit it 100
        // times out of 200 -- 50%.
        const pooled = new PercentValue(50, 100)
            .aggregate(new PercentValue(50, 100), AggregationMode.WeightedRatio)
        expect(pooled).toBeInstanceOf(PercentValue)
        expect(pooled.getNumericValue().unwrap()).toBeCloseTo(50, 9)
    })

    it('CommonDenominator adds the numerators over the common total', () => {
        // Three nodes taking 10%, 20% and 5% of one circuit runtime make up 35% of it.
        const total = new PercentValue(10, 100)
            .aggregate(new PercentValue(20, 100), AggregationMode.CommonDenominator)
            .aggregate(new PercentValue(5, 100), AggregationMode.CommonDenominator)
        expect(total.getNumericValue().unwrap()).toBeCloseTo(35, 9)
    })

    it('CommonDenominator pools when the denominators differ', () => {
        // Different denominators mean the values do not divide by one common total after all;
        // pooling is the reading that stays interpretable.
        const folded = new PercentValue(5, 10)
            .aggregate(new PercentValue(5, 90), AggregationMode.CommonDenominator)
        expect(folded.getNumericValue().unwrap()).toBeCloseTo(10, 9)
    })

    it('both ratio modes ignore a missing reading', () => {
        const half = new PercentValue(1, 2)
        for (const mode of [AggregationMode.WeightedRatio, AggregationMode.CommonDenominator]) {
            expect(half.aggregate(MissingValue.INSTANCE, mode)).toBe(half)
            expect(MissingValue.INSTANCE.aggregate(half, mode)).toBe(half)
        }
    })

    it('rejects a value of another kind, and a mode a percentage has no meaning for', () => {
        const half = new PercentValue(1, 2)
        expect(() => half.aggregate(new CountValue(1), AggregationMode.WeightedRatio)).toThrow()
        expect(() => half.aggregate(half, AggregationMode.Sum)).toThrow()
        expect(() => half.aggregate(half, AggregationMode.Or)).toThrow()
        expect(() => half.aggregate(half, AggregationMode.CommonValue)).toThrow()
    })
})

describe('RatioValue', () => {
    // 100 batches of 10 records, and one batch of 1000 records.
    const many = new RatioValue(new CountValue(1000), 100)
    const one = new RatioValue(new CountValue(1000), 1)

    it('reads as the quotient in the numerator kind', () => {
        expect(many.getNumericValue().unwrap()).toBeCloseTo(10, 9)
        expect(many.toString()).toBe('10')
        const latency = new RatioValue(new TimeValue(2), 4)
        expect(latency.toString()).toBe('500ms')
        const size = new RatioValue(new BytesValue(4096), 2)
        expect(size.toString()).toBe('2KiB')
    })

    const pool = (a: PropertyValue, b: PropertyValue) =>
        a.aggregate(b, AggregationMode.WeightedRatio)

    it('weighs each node by its own denominator', () => {
        // The heavy node dominates: 2000 records over 101 batches, not the midpoint of 10
        // and 1000 (505), and not the largest reading (1000).
        const region = pool(many, one)
        expect(region).toBeInstanceOf(RatioValue)
        expect(region.getNumericValue().unwrap()).toBeCloseTo(2000 / 101, 9)
    })

    it('pooling is independent of the order regions are folded in', () => {
        const third = new RatioValue(new CountValue(40), 4)
        const left = pool(pool(many, one), third)
        const right = pool(many, pool(one, third))
        expect(left.getNumericValue().unwrap())
            .toBeCloseTo(right.getNumericValue().unwrap(), 9)
        expect(left.getNumericValue().unwrap()).toBeCloseTo(2040 / 105, 9)
    })

    it('reads as zero when nothing was observed', () => {
        const idle = new RatioValue(new CountValue(0), 0)
        expect(idle.getNumericValue().unwrap()).toBe(0)
        expect(idle.toString()).toBe('0')
    })

    it('ignores a missing reading', () => {
        expect(pool(many, MissingValue.INSTANCE)).toBe(many)
        expect(pool(MissingValue.INSTANCE, many)).toBe(many)
    })

    it('rejects a value of another kind, and a mode an average has no meaning for', () => {
        expect(() => pool(many, new CountValue(1))).toThrow()
        expect(() => many.aggregate(one, AggregationMode.Sum)).toThrow()
        expect(() => many.aggregate(one, AggregationMode.CommonDenominator)).toThrow()
    })

    it('average across workers is weighted by the denominators', () => {
        const avg = many.average([one])
        expect(avg.getNumericValue().unwrap()).toBeCloseTo(2000 / 101, 9)
    })

    it('average stays missing when no worker reported a ratio', () => {
        expect(MissingValue.INSTANCE.average([MissingValue.INSTANCE])).toBeInstanceOf(MissingValue)
    })

    it('scale multiplies the numerator, leaving the denominator alone', () => {
        const doubled = many.scale(2) as RatioValue
        expect(doubled.getNumericValue().unwrap()).toBeCloseTo(20, 9)
        expect(doubled.denominator).toBe(100)
    })

})

describe('PropertyValue.min', () => {
    it('picks the smaller reading', () => {
        expect(new CountValue(5).min(new CountValue(1)).getNumericValue().unwrap()).toBe(1)
        expect(new CountValue(1).min(new CountValue(5)).getNumericValue().unwrap()).toBe(1)
    })

    it('a missing reading never wins, from either side', () => {
        // A missing value compares below every reading, so a plain comparison would return it
        // and erase the readings of the other nodes.
        const five = new CountValue(5)
        expect(five.min(MissingValue.INSTANCE)).toBe(five)
        expect(MissingValue.INSTANCE.min(five)).toBe(five)
        expect(MissingValue.INSTANCE.min(MissingValue.INSTANCE)).toBeInstanceOf(MissingValue)
    })

    it('max likewise ignores a missing reading', () => {
        const five = new CountValue(5)
        expect(five.max(MissingValue.INSTANCE)).toBe(five)
        expect(MissingValue.INSTANCE.max(five)).toBe(five)
    })
})

describe('PropertyValue.aggregate', () => {
    const a = new CountValue(4)
    const b = new CountValue(6)

    it('routes each mode a count supports to its operation', () => {
        expect(a.aggregate(b, AggregationMode.Sum).getNumericValue().unwrap()).toBe(10)
        expect(a.aggregate(b, AggregationMode.Min).getNumericValue().unwrap()).toBe(4)
        expect(a.aggregate(b, AggregationMode.Max).getNumericValue().unwrap()).toBe(6)
    })

    it('rejects a reading of another kind under every mode', () => {
        // Min and Max are implemented once for every kind, so they used to fold a count with a
        // byte size and return whichever compared smaller -- changing the kind of the result.
        for (const mode of [AggregationMode.Min, AggregationMode.Max, AggregationMode.Sum]) {
            expect(() => a.aggregate(new BytesValue(3), mode), AggregationMode[mode]).toThrow()
        }
        expect(() => new RatioValue(new CountValue(10), 2)
            .aggregate(a, AggregationMode.Min)).toThrow()
        // A missing reading is not another kind: it contributes nothing to any fold.
        expect(a.aggregate(MissingValue.INSTANCE, AggregationMode.Min)).toBe(a)
        expect(a.aggregate(MissingValue.INSTANCE, AggregationMode.Max)).toBe(a)
    })

    it('rejects the modes a count has no meaning for', () => {
        for (const mode of [AggregationMode.WeightedRatio, AggregationMode.CommonDenominator,
        AggregationMode.Or, AggregationMode.CommonValue]) {
            expect(() => a.aggregate(b, mode), AggregationMode[mode]).toThrow()
        }
    })

    it('rejects a mode it does not know', () => {
        expect(() => a.aggregate(b, 99 as AggregationMode)).toThrow()
    })

    it('a flag ORs and a setting agrees, and neither orders', () => {
        const yes = new BooleanValue(true)
        const no = new BooleanValue(false)
        expect((yes.aggregate(no, AggregationMode.Or) as BooleanValue).value).toBe(true)
        expect((no.aggregate(no, AggregationMode.Or) as BooleanValue).value).toBe(false)
        const one = new StringValue('round-robin')
        expect(one.aggregate(one, AggregationMode.CommonValue)).toBe(one)
        expect(one.aggregate(new StringValue('least-loaded'), AggregationMode.CommonValue)
            .toString()).toBe('<multiple values>')
        expect(() => yes.aggregate(no, AggregationMode.Min)).toThrow()
        expect(() => one.aggregate(one, AggregationMode.Max)).toThrow()
    })

    it('a missing reading contributes nothing under every mode', () => {
        for (const mode of [AggregationMode.Sum, AggregationMode.WeightedRatio,
        AggregationMode.CommonDenominator, AggregationMode.Min, AggregationMode.Max,
        AggregationMode.Or, AggregationMode.CommonValue]) {
            expect(MissingValue.INSTANCE.aggregate(a, mode), AggregationMode[mode]).toBe(a)
        }
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

    // For booleans/enum strings, .average() returns the mode across workers so the "Avg"
    // column shows the prevailing reading instead of N/A. Ties keep the first reading.
    it('BooleanValue.average returns the more common value', () => {
        const avg = new BooleanValue(true).average([
            new BooleanValue(true),
            new BooleanValue(false)
        ])
        expect(avg).toBeInstanceOf(BooleanValue)
        expect((avg as BooleanValue).value).toBe(true)
        expect(avg.toString()).toBe('true')
    })

    it('BooleanValue.average keeps the first reading on a tie', () => {
        // 1 true + 1 false → tie → keep `this` (false here, since average is called on false).
        const avg = new BooleanValue(false).average([new BooleanValue(true)])
        expect(avg).toBeInstanceOf(BooleanValue)
        expect((avg as BooleanValue).value).toBe(false)
    })

    it('StringValue.average returns the most common value', () => {
        const avg = new StringValue('round-robin').average([
            new StringValue('round-robin'),
            new StringValue('least-loaded')
        ])
        expect(avg).toBeInstanceOf(StringValue)
        expect(avg.toString()).toBe('round-robin')
    })

    it('StringValue.average keeps the first-seen value on a tie', () => {
        // 1 vs 1 → tie → first-seen ("least-loaded") wins.
        const avg = new StringValue('least-loaded').average([new StringValue('round-robin')])
        expect(avg).toBeInstanceOf(StringValue)
        expect(avg.toString()).toBe('least-loaded')
    })

    it('Boolean/String average skips MissingValue neighbours', () => {
        const b = new BooleanValue(true).average([MissingValue.INSTANCE, new BooleanValue(true)])
        expect((b as BooleanValue).value).toBe(true)
        const s = new StringValue('A').average([MissingValue.INSTANCE, new StringValue('A')])
        expect(s.toString()).toBe('A')
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
    const samples: PropertyValue[] = [
        new CountValue(1),
        new BytesValue(1),
        new TimeValue(1),
        new PercentValue(50, 100),
        new RatioValue(new CountValue(10), 2),
        new StringValue('x'),
        new BooleanValue(true),
        MissingValue.INSTANCE
    ]

    it('every concrete subclass implements average and toString without throwing', () => {
        for (const v of samples) {
            expect(typeof v.toString()).toBe('string')
            // Self-average: every kind should accept an empty `others` array without throwing.
            expect(() => v.average([])).not.toThrow()
        }
    })

    it('every concrete subclass scales', () => {
        for (const v of samples) {
            expect(v.scale(2), v.constructor.name).toBeInstanceOf(PropertyValue)
        }
    })

    // Which modes each kind answers to.
    const ALL = [AggregationMode.Sum, AggregationMode.WeightedRatio,
    AggregationMode.CommonDenominator, AggregationMode.Or, AggregationMode.CommonValue]
    const ACCEPTED = new Map<string, AggregationMode[]>([
        ['CountValue', [AggregationMode.Sum]],
        ['BytesValue', [AggregationMode.Sum]],
        ['TimeValue', [AggregationMode.Sum]],
        ['PercentValue', [AggregationMode.WeightedRatio, AggregationMode.CommonDenominator]],
        ['RatioValue', [AggregationMode.WeightedRatio]],
        ['StringValue', [AggregationMode.CommonValue]],
        ['BooleanValue', [AggregationMode.Or]],
        ['MissingValue', ALL]
    ])

    it('every kind answers to exactly the modes that mean something for it', () => {
        for (const v of samples) {
            const accepted = ALL.filter(mode => {
                try {
                    v.aggregate(v, mode)
                    return true
                } catch {
                    return false
                }
            })
            expect(accepted.map(m => AggregationMode[m]), v.constructor.name).toEqual(
                ACCEPTED.get(v.constructor.name)!.map(m => AggregationMode[m]))
        }
    })

    it('folding a value with itself keeps its kind', () => {
        for (const v of samples) {
            for (const mode of ACCEPTED.get(v.constructor.name)!) {
                expect(v.aggregate(v, mode), v.constructor.name + " " + AggregationMode[mode])
                    .toBeInstanceOf(v.constructor as typeof PropertyValue)
            }
        }
    })

    it('scaling a magnitude by 2 doubles it', () => {
        expect(new CountValue(3).scale(2).getNumericValue().unwrap()).toBe(6)
        expect(new BytesValue(3).scale(2).getNumericValue().unwrap()).toBe(6)
        expect(new TimeValue(3).scale(2).getNumericValue().unwrap()).toBe(6)
        expect(new PercentValue(3, 100).scale(2).getNumericValue().unwrap()).toBeCloseTo(6, 9)
    })

    it('kinds without a magnitude scale to themselves', () => {
        const s = new StringValue('x')
        const b = new BooleanValue(true)
        expect(s.scale(2)).toBe(s)
        expect(b.scale(2)).toBe(b)
        expect(MissingValue.INSTANCE.scale(2)).toBe(MissingValue.INSTANCE)
    })
})

// The profiler colors per-worker bars against the metric's range over the nodes on display, not
// against the current node's own spread: `CircuitProfile.displayScales` unions each drawn node's
// per-worker values and the rendering calls `percents` on the result. These tests pin that
// composition, so a value's color depends on the other nodes it is shown beside.
describe('NumericRange cross-node normalization', () => {
    // Two nodes: A workers [10, 20], B workers [100, 200]. The range over both is [10, 200].
    const nodeA = NumericRange.getRange([10, 20])
    const nodeB = NumericRange.getRange([100, 200])
    const global = nodeA.union(nodeB)

    it('union spans the min and max across every node', () => {
        expect(global.min).toBe(10)
        expect(global.max).toBe(200)
    })

    it('normalizes a value against the global range, not its own node', () => {
        // A's local max (20) is near the bottom of the circuit, so it colors cool, not hot.
        // Local normalization would place 20 at 100% of node A's [10, 20] range — the regression.
        expect(global.percents(20)).toBeCloseTo((100 * (20 - 10)) / (200 - 10), 5)
        expect(global.percents(20)).toBeLessThan(10)
        expect(nodeA.percents(20)).toBe(100)
        expect(global.percents(200)).toBe(100)
    })

    it('a value has the same color wherever it appears, regardless of node-local spread', () => {
        // 100 sits at the same global percentile whether reached from node A or node B.
        expect(global.percents(100)).toBeCloseTo(nodeA.union(nodeB).percents(100), 5)
    })

    it('degenerate ranges collapse to a single point value', () => {
        // Empty (no numeric readings) unions away; a single distinct value yields a point range.
        const empty = NumericRange.empty()
        expect(empty.union(nodeA)).toEqual(nodeA)
        const point = NumericRange.getRange([42, 42])
        expect(point.isPoint()).toBe(true)
    })
})

// Every parse site states how its metric folds.
describe('Measurement.parseValues tags the new-format metrics', () => {
    const duration = (seconds: number) =>
        ({ type: 'duration', value: { secs: Math.floor(seconds), nanos: 0 } })
    const count = (value: number) => ({ type: 'count', value })

    const parse = (metric: object): Map<string, Measurement> => {
        const parsed = Measurement.parseValues(metric as any)
        return new Map(parsed.map((m: Measurement) => [m.property, m]))
    }

    it('tags a batch-size summary by what each field reports', () => {
        const parsed = parse({
            metric_id: 'input_batches_stats',
            value: {
                batches_count: count(4), min_records_count: count(1),
                max_records_count: count(9), avg_records_count: count(5),
                total_records_count: count(20)
            }
        })
        const modes = new Map([...parsed].map(([key, m]) => [key, m.aggregation]))
        expect(modes.get('input_batches_stats.count')).toBe(AggregationMode.Sum)
        expect(modes.get('input_batches_stats.record_count')).toBe(AggregationMode.Sum)
        expect(modes.get('input_batches_stats.min_size')).toBe(AggregationMode.Min)
        expect(modes.get('input_batches_stats.max_size')).toBe(AggregationMode.Max)
        expect(modes.get('input_batches_stats.avg_size')).toBe(AggregationMode.WeightedRatio)
        // The average keeps its terms: 20 records over 4 batches.
        const avg = parsed.get('input_batches_stats.avg_size')!.value.unwrap()
        expect(avg).toBeInstanceOf(RatioValue)
        expect(avg.getNumericValue().unwrap()).toBe(5)
    })

    it('tags a node runtime part by the total it divides by, other percents by their own terms', () => {
        const percent = (id: string) => parse({
            metric_id: id, value: { type: 'percent', value: { numerator: 1, denominator: 4 } }
        }).get(id)!.aggregation
        expect(percent('runtime_percent')).toBe(AggregationMode.CommonDenominator)
        expect(percent('nonblocking_percent')).toBe(AggregationMode.WeightedRatio)
        expect(percent('bloom_filter_hit_rate_percent')).toBe(AggregationMode.WeightedRatio)
    })

    it('does not add up a wall clock that every worker and node repeats', () => {
        // The circuit's elapsed time is one reading the profile repeats everywhere. Adding it up
        // reported it multiplied by the worker count -- days, for a run of hours.
        const elapsed = (id: string) => parse({
            metric_id: id, value: { type: 'duration', value: { secs: 60, nanos: 0 } }
        }).get(id)!.aggregation
        expect(elapsed('circuit_runtime_elapsed_seconds')).toBe(AggregationMode.Max)
        // A duration a node spent working is its own, and adds up.
        expect(elapsed('runtime_seconds')).toBe(AggregationMode.Sum)
        expect(elapsed('circuit_runtime_seconds')).toBe(AggregationMode.Sum)
    })

    it('takes the largest cache occupancy rather than adding the readings up', () => {
        const parsed = parse({
            metric_id: 'foreground_cache_occupancy',
            value: { max: { type: 'bytes', value: 1024 }, used: { type: 'bytes', value: 512 } }
        })
        // One cache per worker, reported again by every node inside it.
        expect(parsed.get('foreground_cache_occupancy.max')!.aggregation).toBe(AggregationMode.Max)
        expect(parsed.get('foreground_cache_occupancy.used')!.aggregation).toBe(AggregationMode.Max)
    })

    it('reads the per-step merge averages the runtime actually reports', () => {
        // The runtime serializes `avg_step_seconds` and `avg_step_cpu_seconds`; reading a field
        // by another name dropped both readings without a word.
        const parsed = parse({
            metric_id: 'completed_merges',
            labels: [['slot', '0']],
            value: {
                avg_step_seconds: duration(2), avg_step_cpu_seconds: duration(1),
                batches: count(10), merges: count(3), steps: count(4)
            }
        })
        const elapsed = parsed.get('completed_merges.slot:0.avg_step_seconds')!
        expect(elapsed.aggregation).toBe(AggregationMode.WeightedRatio)
        expect(elapsed.value.unwrap().getNumericValue().unwrap()).toBeCloseTo(2, 9)
        expect(parsed.get('completed_merges.slot:0.avg_step_cpu_seconds')!.value.unwrap()
            .getNumericValue().unwrap()).toBeCloseTo(1, 9)
        expect(parsed.get('completed_merges.slot:0.steps')!.aggregation)
            .toBe(AggregationMode.Sum)
        const other = parse({
            metric_id: 'completed_merges',
            labels: [['slot', '1']],
            value: {
                avg_step_seconds: duration(1), avg_step_cpu_seconds: duration(1),
                batches: count(1), merges: count(1), steps: count(1)
            }
        }).get('completed_merges.slot:1.avg_step_seconds')!
        const folded = elapsed.value.unwrap()
            .aggregate(other.value.unwrap(), AggregationMode.WeightedRatio)
        expect(folded.getNumericValue().unwrap()).toBeCloseTo((2 * 4 + 1) / 5, 9)
    })
})

// TODO: remove together with the rest of the legacy parsing path.
describe('Measurement.parseLegacyValues tags the legacy metrics', () => {
    const parsed = (datum: unknown[]) => Measurement.parseLegacyValues(datum as any)[0]!

    const CASES: Array<[unknown[], string, AggregationMode]> = [
        [['time%', [10, 1000]], 'PercentValue', AggregationMode.CommonDenominator],
        [['merge reduction', [1, 2]], 'PercentValue', AggregationMode.WeightedRatio],
        [['Bloom filter hit rate', [1, 2]], 'PercentValue', AggregationMode.WeightedRatio],
        [['batches', 5], 'CountValue', AggregationMode.Sum],
        [['storage size', 5], 'CountValue', AggregationMode.Sum],
        [['time', { secs: 1, nanos: 0 }], 'TimeValue', AggregationMode.Sum],
        [['runtime_elapsed', { secs: 1, nanos: 0 }], 'TimeValue', AggregationMode.Max],
        [['balancer policy', 'round-robin'], 'StringValue', AggregationMode.CommonValue],
        [['rebalancing in progress', true], 'BooleanValue', AggregationMode.Or]
    ]

    for (const [datum, kind, mode] of CASES) {
        it(`reads ${datum[0]} as a ${kind} folded with ${AggregationMode[mode]}`, () => {
            const m = parsed(datum)
            expect(m.value.unwrap().constructor.name).toBe(kind)
            expect(m.aggregation).toBe(mode)
        })
    }

    // Batch-size summaries arrive nested under one name, and the entries inside it are spelled
    // with a slash. Joining them with a dot left all five unparsed and uncategorized.
    it('reads the nested batch-size summary that the format actually sends', () => {
        const parsed = Measurement.parseLegacyValues(['input batches', {
            entries: [['batches', 5], ['min size', 3], ['max size', 9], ['avg size', 7],
            ['total records', 35]]
        }] as any)
        const read = parsed.map(m => [m.property, m.value.isSome()
            ? m.value.unwrap().constructor.name : 'unparsed', AggregationMode[m.aggregation]])
        expect(read).toEqual([
            ['input batches/batches', 'CountValue', 'Sum'],
            ['input batches/min size', 'CountValue', 'Min'],
            ['input batches/max size', 'CountValue', 'Max'],
            ['input batches/avg size', 'RatioValue', 'WeightedRatio'],
            ['input batches/total records', 'CountValue', 'Sum']
        ])
        // The names have to match the category table too, or the metrics land under "Other".
        expect(measurementCategory('input batches/min size')).toBe('storage')
    })

    it('leaves a metric it cannot turn into a number unparsed', () => {
        expect(parsed(['key distribution', [1, 2, 3]]).value.isNone()).toBe(true)
    })
})

// A region's value for a metric is folded from the nodes it contains
describe('region aggregation', () => {
    const count = (value: number) => ({ type: 'count', value })

    type Leaf = {
        batches: number, min: number, max: number, records: number,
        runtime: [number, number], nonblocking: [number, number], memory: number
    }

    const readings = (leaf: Leaf) => [
        {
            metric_id: 'input_batches_stats',
            value: {
                batches_count: count(leaf.batches),
                min_records_count: count(leaf.min),
                max_records_count: count(leaf.max),
                // The runtime reports the quotient too, rounded down; the parser recomputes it.
                avg_records_count: count(Math.floor(leaf.records / leaf.batches)),
                total_records_count: count(leaf.records)
            }
        },
        {
            metric_id: 'runtime_percent',
            value: {
                type: 'percent',
                value: { numerator: leaf.runtime[0], denominator: leaf.runtime[1] }
            }
        },
        {
            metric_id: 'nonblocking_percent',
            value: {
                type: 'percent',
                value: { numerator: leaf.nonblocking[0], denominator: leaf.nonblocking[1] }
            }
        },
        { metric_id: 'used_memory_bytes', value: { type: 'bytes', value: leaf.memory } }
    ]

    // Region r1 holds two operators and a nested region r2 holding a third.
    const graph = {
        nodes: {
            id: 'n', label: 'root', nodes: [{
                Cluster: {
                    id: 'r1', label: 'region', nodes: [
                        { Simple: { id: 'n1', label: 'op1' } },
                        { Simple: { id: 'n2', label: 'op2' } },
                        {
                            Cluster: {
                                id: 'r2', label: 'subregion',
                                nodes: [{ Simple: { id: 'n3', label: 'op3' } }]
                            }
                        }
                    ]
                }
            }]
        },
        edges: []
    }

    const leaves: Record<string, Leaf> = {
        n1: {
            batches: 10, min: 5, max: 50, records: 100,
            runtime: [10, 1000], nonblocking: [30, 60], memory: 1000
        },
        n2: {
            batches: 1, min: 1, max: 20, records: 1000,
            runtime: [20, 1000], nonblocking: [10, 40], memory: 2000
        },
        n3: {
            batches: 4, min: 7, max: 70, records: 40,
            runtime: [5, 1000], nonblocking: [5, 10], memory: 4000
        }
    }

    const parse = (workers: Array<Record<string, Leaf>>) => {
        const json = {
            metrics: [],
            worker_profiles: workers.map(w => ({
                metadata: Object.fromEntries(
                    Object.entries(w).map(([id, leaf]) => [id, readings(leaf)]))
            })),
            graph
        }
        return CircuitProfile.fromJson(json as unknown as JsonProfiles).profile
    }

    const value = (profile: CircuitProfile, region: string, metric: string, worker = 0) =>
        profile.complexNodes.get(region).unwrap().getMeasurements(metric)[worker]!

    const numeric = (profile: CircuitProfile, region: string, metric: string, worker = 0) =>
        value(profile, region, metric, worker).getNumericValue().unwrap()

    it('takes the smallest reading for a reported minimum', () => {
        const profile = parse([leaves])
        // min(5, 1, 7)
        expect(numeric(profile, 'r1', 'input_batches_stats.min_size')).toBe(1)
        expect(numeric(profile, 'r2', 'input_batches_stats.min_size')).toBe(7)
    })

    it('takes the largest reading for a reported maximum', () => {
        const profile = parse([leaves])
        expect(numeric(profile, 'r1', 'input_batches_stats.max_size')).toBe(70)
    })

    it('weighs an average by what each node observed', () => {
        const profile = parse([leaves])
        // 1140 records over 15 batches
        expect(value(profile, 'r1', 'input_batches_stats.avg_size')).toBeInstanceOf(RatioValue)
        expect(numeric(profile, 'r1', 'input_batches_stats.avg_size')).toBeCloseTo(1140 / 15, 9)
        expect(value(profile, 'r1', 'input_batches_stats.avg_size').toString()).toBe('76')
    })

    it('adds up the shares of the circuit runtime', () => {
        const profile = parse([leaves])
        // 1% + 2% + 0.5%
        expect(numeric(profile, 'r1', 'runtime_percent')).toBeCloseTo(3.5, 9)
        expect(value(profile, 'r1', 'runtime_percent').toString()).toBe('3.5%')
    })

    it('pools the terms of a ratio the nodes computed for themselves', () => {
        const profile = parse([leaves])
        // 45 CPU-seconds out of 110 elapsed; the old rule reported the largest rate, 50%.
        expect(numeric(profile, 'r1', 'nonblocking_percent')).toBeCloseTo(100 * 45 / 110, 9)
    })

    it('still adds up the metrics that add up, counting each node once', () => {
        const profile = parse([leaves])
        expect(numeric(profile, 'r1', 'used_memory_bytes')).toBe(7000)
        expect(numeric(profile, 'r1', 'input_batches_stats.count')).toBe(15)
        expect(numeric(profile, 'r1', 'input_batches_stats.record_count')).toBe(1140)
        // The nested region must not be folded a second time through its own children.
        expect(numeric(profile, 'r2', 'used_memory_bytes')).toBe(4000)
    })

    it('folds each worker separately', () => {
        // Worker 0's smallest batch is in n1, worker 1's is in n2.
        const swapped = {
            n1: { ...leaves.n1!, min: 60 },
            n2: { ...leaves.n2!, min: 3 },
            n3: leaves.n3!
        }
        const profile = parse([leaves, swapped])
        expect(numeric(profile, 'r1', 'input_batches_stats.min_size', 0)).toBe(1)
        expect(numeric(profile, 'r1', 'input_batches_stats.min_size', 1)).toBe(3)
    })
})

// A node's readings for one metric add up across workers only when the metric adds up. Totalling
// a rate, a reported minimum or a flag would print a number with no meaning, so those have none.
describe('SimpleNode.totalOf', () => {
    const node = () => new SimpleNode('n1', 'op', 3)

    const withMetric = (property: string, aggregation: AggregationMode,
        values: PropertyValue[]) => {
        const n = node()
        values.forEach((value, worker) =>
            n.addMeasurement(new Measurement(property, Option.some(value), aggregation), worker))
        return n
    }

    it('adds up counts, byte sizes and durations', () => {
        const counts = withMetric('records', AggregationMode.Sum,
            [new CountValue(1), new CountValue(2), new CountValue(3)])
        expect(counts.totalOf('records', counts.getMeasurements('records')).unwrap()
            .getNumericValue().unwrap()).toBe(6)

        const bytes = withMetric('memory', AggregationMode.Sum,
            [new BytesValue(1024), new BytesValue(1024)])
        const totalBytes = bytes.totalOf('memory', bytes.getMeasurements('memory')).unwrap()
        expect(totalBytes).toBeInstanceOf(BytesValue)
        expect(totalBytes.toString()).toBe('2KiB')

        const times = withMetric('runtime', AggregationMode.Sum,
            [new TimeValue(0.5), new TimeValue(1.5)])
        expect(times.totalOf('runtime', times.getMeasurements('runtime')).unwrap().toString())
            .toBe('2s')
    })

    it('has no total for the metrics that do not add up', () => {
        const cases: Array<[string, AggregationMode, PropertyValue[]]> = [
            ['hit_rate', AggregationMode.WeightedRatio,
                [new PercentValue(1, 2), new PercentValue(1, 4)]],
            ['runtime_percent', AggregationMode.CommonDenominator,
                [new PercentValue(1, 10), new PercentValue(2, 10)]],
            ['avg_size', AggregationMode.WeightedRatio,
                [new RatioValue(new CountValue(10), 2), new RatioValue(new CountValue(9), 3)]],
            ['min_size', AggregationMode.Min, [new CountValue(1), new CountValue(5)]],
            ['max_size', AggregationMode.Max, [new CountValue(1), new CountValue(5)]],
            ['rebalancing', AggregationMode.Or, [new BooleanValue(true), new BooleanValue(false)]],
            ['policy', AggregationMode.CommonValue,
                [new StringValue('round-robin'), new StringValue('round-robin')]]
        ]
        for (const [property, mode, values] of cases) {
            const n = withMetric(property, mode, values)
            expect(n.totalOf(property, n.getMeasurements(property)).isNone(), property).toBe(true)
        }
    })

    it('skips workers that reported nothing, and totals only the workers it is given', () => {
        const n = withMetric('records', AggregationMode.Sum,
            [new CountValue(1), MissingValue.INSTANCE, new CountValue(3)])
        const all = n.getMeasurements('records')
        expect(n.totalOf('records', all).unwrap().getNumericValue().unwrap()).toBe(4)
        // The caller passes the workers it displays; the total covers those only.
        expect(n.totalOf('records', all.slice(0, 1)).unwrap().getNumericValue().unwrap()).toBe(1)
    })

    it('has no total when no worker reported the metric', () => {
        const n = withMetric('records', AggregationMode.Sum,
            [MissingValue.INSTANCE, MissingValue.INSTANCE])
        expect(n.totalOf('records', n.getMeasurements('records')).isNone()).toBe(true)
        // A metric this node never carried has none either.
        expect(n.totalOf('absent', []).isNone()).toBe(true)
    })
})

// In the per-node view the Total column shades a node's total against the largest total any node
// reports for that metric, so the node holding the most is the reddest. The scale needs its own
// maximum: a total is larger than any single worker reading, and a region holds more than any
// node inside it, so a scale built from leaf worker readings pins every region at full red.
// The toplevel node holds the whole circuit, so it reports the largest total of every metric and
// `totalShare` would place all of them at 100 -- the bug where the overview came out uniformly
// red. Its totals are placed against the others of their category instead.
describe('categoryShares', () => {
    // 'total size', 'allocated bytes' and 'batches' are all in the memory category; 'time' is in
    // CPU, so it is scaled on its own.
    const totals = new Map<string, PropertyValue>([
        ['total size', new BytesValue(13_260_000_000)],
        ['allocated bytes', new CountValue(5_930_000)],
        ['batches', new CountValue(366)],
        ['time', new TimeValue(170)]
    ])

    it('spreads totals that span orders of magnitude', () => {
        const shares = categoryShares(totals)
        // Log scale: the largest saturates and the smallest stays well clear of it, where a
        // linear scale would put 366 out of 13 billion at 0.
        expect(shares.get('total size')).toBe(100)
        expect(shares.get('allocated bytes')!).toBeGreaterThan(60)
        expect(shares.get('batches')!).toBeGreaterThan(20)
        expect(shares.get('batches')!).toBeLessThan(30)
    })

    it('scales each category on its own', () => {
        // The only CPU total, so it is that category's maximum however small it looks beside
        // the byte counts.
        expect(measurementCategory('time')).not.toBe(measurementCategory('total size'))
        expect(categoryShares(totals).get('time')).toBe(100)
    })

    it('shades nothing when a category measured nothing', () => {
        const shares = categoryShares(new Map([['total size', new BytesValue(0)]]))
        expect(shares.get('total size')).toBe(0)
    })
})

describe('CircuitProfile.displayScales', () => {
    const readings = (bytes: number) =>
        [{ metric_id: 'used_memory_bytes', value: { type: 'bytes', value: bytes } },
        {
            metric_id: 'input_batches_stats',
            value: {
                batches_count: { type: 'count', value: 2 },
                min_records_count: { type: 'count', value: 1 },
                max_records_count: { type: 'count', value: 9 },
                avg_records_count: { type: 'count', value: 5 },
                total_records_count: { type: 'count', value: 10 }
            }
        }]

    // Two operators over two workers each, inside a region: totals are 2 and 100 bytes for the
    // leaves, 102 for the region that holds them.
    const profile = (() => {
        const metadata = { small: readings(1), large: readings(50) }
        const json = {
            metrics: [],
            worker_profiles: [{ metadata }, { metadata }],
            graph: {
                nodes: {
                    id: 'n', label: 'root', nodes: [{
                        Cluster: {
                            id: 'r', label: 'region', nodes: [
                                { Simple: { id: 'small', label: 'small' } },
                                { Simple: { id: 'large', label: 'large' } }
                            ]
                        }
                    }]
                },
                edges: []
            }
        }
        return CircuitProfile.fromJson(json as unknown as JsonProfiles).profile
    })()

    const all = (values: PropertyValue[]) => values
    const memory = (nodes: string[]) =>
        profile.displayScales(nodes, all).get('used_memory_bytes')?.maximum

    it('takes the largest total among the nodes it is given', () => {
        expect(memory(['small', 'large'])).toBe(100)
        expect(memory(['small'])).toBe(2)
    })

    it('scales by the region when it is collapsed and by its nodes when it is expanded', () => {
        // Collapsed, the region stands for everything inside it and sets the scale itself.
        expect(memory(['r'])).toBe(102)
        // Expanded, the nodes inside it are what is drawn, so they set the scale instead.
        expect(memory(['small', 'large'])).toBe(100)
    })

    it('ranges over worker readings and maxes over totals, from the same pass', () => {
        const scales = profile.displayScales(['small', 'large'], all)
        // Worker readings run 1 to 50; the largest node total is 100. Coloring a worker cell
        // against the totals scale would leave every bar in the bottom half of the range.
        expect(scales.get('used_memory_bytes')!.range.min).toBe(1)
        expect(scales.get('used_memory_bytes')!.range.max).toBe(50)
        expect(scales.get('used_memory_bytes')!.maximum).toBe(100)
    })

    it('covers only the workers on display', () => {
        const firstWorker = (values: PropertyValue[]) => values.slice(0, 1)
        const scales = profile.displayScales(['small', 'large'], firstWorker)
        expect(scales.get('used_memory_bytes')!.maximum).toBe(50)
        expect(scales.get('used_memory_bytes')!.range.max).toBe(50)
    })

    it('has no maximum for a metric that does not add up, but still ranges it', () => {
        const scales = profile.displayScales(['small', 'large', 'absent'], all)
        expect(scales.get('input_batches_stats.min_size')!.maximum).toBeUndefined()
        // The bars still need a scale for the metrics that have no total.
        expect(scales.get('input_batches_stats.min_size')!.range.max).toBe(1)
        // Two batches per worker over two workers: 4 per node, and the maximum is per node.
        expect(scales.get('input_batches_stats.count')!.maximum).toBe(4)
    })
})

describe('totalShare', () => {
    it('is the total as a percentage of the largest on display', () => {
        expect(totalShare(new BytesValue(100), 100)).toBe(100)
        expect(totalShare(new BytesValue(50), 100)).toBe(50)
        expect(totalShare(new BytesValue(2), 100)).toBe(2)
        expect(totalShare(new BytesValue(0), 100)).toBe(0)
    })

    it('shades nothing when there is nothing to compare against', () => {
        expect(totalShare(new BytesValue(10), undefined)).toBe(0)
        expect(totalShare(new BytesValue(10), 0)).toBe(0)
        expect(totalShare(MissingValue.INSTANCE, 100)).toBe(0)
    })

    it('clamps a total that exceeds the largest on display', () => {
        // An expanded region is left out of the scale, yet its own row still has to render.
        expect(totalShare(new BytesValue(500), 100)).toBe(100)
    })
})

// The "top nodes" list ranks by the metric's total over a node's workers where the metric adds
// up. Ranking by the largest single reading, as it did before, cannot tell a node that is busy on
// every worker from one that is busy on a single worker.
describe('CircuitProfile.rankNodes', () => {
    // Four workers. `spread` works on all of them, `hot` only on the first, harder.
    const profile = (property: string, mode: AggregationMode) => {
        const p = new CircuitProfile(4, 'n')
        const add = (id: string, values: number[]) => {
            const node = new SimpleNode(id, id, 4)
            values.forEach((v, worker) =>
                node.addMeasurement(
                    new Measurement(property, Option.some(new CountValue(v)), mode), worker))
            p.simpleNodes.set(id, node)
        }
        add('spread', [30, 30, 30, 30])
        add('hot', [50, 0, 0, 0])
        return p
    }

    const nodes = [{ id: 'spread', label: 'spread' }, { id: 'hot', label: 'hot' }]

    it('ranks by the total where the metric adds up', () => {
        const ranked = profile('records', AggregationMode.Sum).rankNodes('records', nodes)
        // 120 records against 50: the busy-everywhere node comes first.
        expect(ranked.map((r) => r.nodeId)).toEqual(['spread', 'hot'])
        expect(ranked[0]!.label).toBe('120')
        expect(ranked[1]!.label).toBe('50')
        // Totals are normalized against each other, so the largest fills the bar.
        expect(ranked[0]!.normalizedValue).toBe(100)
    })

    it('ranks by the largest worker reading where the metric does not add up', () => {
        const ranked = profile('min_size', AggregationMode.Min).rankNodes('min_size', nodes)
        expect(ranked.map((r) => r.nodeId)).toEqual(['hot', 'spread'])
        expect(ranked[0]!.label).toBe('50')
        expect(ranked[1]!.label).toBe('30')
    })

    it('leaves out nodes that never reported the metric', () => {
        const p = profile('records', AggregationMode.Sum)
        p.simpleNodes.set('silent', new SimpleNode('silent', 'silent', 4))
        const ranked = p.rankNodes('records', [...nodes, { id: 'silent', label: 'silent' }])
        expect(ranked.map((r) => r.nodeId)).toEqual(['spread', 'hot'])
    })

    it('gives every node the same standing when the totals are equal', () => {
        const p = new CircuitProfile(2, 'n')
        for (const id of ['a', 'b']) {
            const node = new SimpleNode(id, id, 2)
            for (const worker of [0, 1]) {
                node.addMeasurement(new Measurement(
                    'records', Option.some(new CountValue(7)), AggregationMode.Sum), worker)
            }
            p.simpleNodes.set(id, node)
        }
        const ranked = p.rankNodes('records', [{ id: 'a', label: 'a' }, { id: 'b', label: 'b' }])
        // A single distinct total is a point range, which carries no standing to show.
        expect(ranked.map((r) => r.normalizedValue)).toEqual([0, 0])
    })
})

describe('ComplexNode.leafCount', () => {
    // A region's badge reports the primitive operators hidden inside it, at any nesting depth -
    // counting immediate children instead would report 2 for `outer` below.
    const simple = (id: string, label: string) => ({ Simple: { id, label } })
    const cluster = (id: string, label: string, nodes: unknown[]) => ({ Cluster: { id, label, nodes } })

    const parse = () =>
        CircuitProfile.fromJson({
            metrics: [],
            worker_profiles: [{ metadata: {} }],
            graph: {
                nodes: {
                    id: 'n',
                    label: 'circuit',
                    nodes: [
                        simple('n1', 'source'),
                        cluster('outer', 'region', [
                            simple('n2', 'map'),
                            cluster('inner', 'subregion', [simple('n3', 'join'), simple('n4', 'filter')])
                        ])
                    ]
                },
                edges: []
            }
        } as never).profile

    it('counts primitive operators at any depth, not immediate children', () => {
        const profile = parse()
        expect(profile.complexNodes.get('inner').unwrap().leafCount).toBe(2)
        expect(profile.complexNodes.get('outer').unwrap().leafCount).toBe(3)
    })

    it('leaves primitive operators and the toplevel graph node without a count', () => {
        const profile = parse()
        // Primitive operators carry no count, so they get no badge...
        expect(profile.simpleNodes.get('n1').unwrap()).not.toHaveProperty('leafCount')
        // ...and neither does the toplevel node, which is never drawn.
        expect(profile.complexNodes.get('n').unwrap().leafCount).toBe(0)
    })
})

describe('CircuitProfile.byName', () => {
    const mirNode = (persistent_id: string, table: string | null, view: string | null) => ({
        operation: 'op', table, view, inputs: [], calcite: {}, positions: [], persistent_id
    })

    const makeProfile = () => {
        const profile = new CircuitProfile(1, 'n')
        const source = new SimpleNode('n1', 'source', 1)
        const sink = new SimpleNode('n2', 'sink', 1)
        const port = new SimpleNode('n3', 'source', 1)
        for (const [pid, node] of [['abc123', source], ['def456', sink], ['789fed', port]] as const) {
            profile.simpleNodes.set(node.id, node)
            profile.byPersistentId.set(pid, node)
        }
        const dataflow: Dataflow = {
            calcite_plan: {},
            mir: {
                s1: mirNode('abc123', 'CUSTOMERS', null),
                s2: mirNode('def456', null, 'report'),
                s3: mirNode('789fed', 'port', null)
            }
        }
        profile.setDataflow(dataflow)
        return { profile, source, sink, port }
    }

    it('indexes input tables and output views by lowercase name', () => {
        const { profile, source, sink } = makeProfile()
        // Quoted uppercase table names are found by their lowercase key
        expect(profile.byName.get('customers').unwrap()).toBe(source)
        expect(profile.byName.get('report').unwrap()).toBe(sink)
        expect(profile.byName.get('missing').isNone()).toBe(true)
    })

    it('findByName falls back to a substring match', () => {
        const { profile, source, sink } = makeProfile()
        expect(profile.findByName('CUSTOM').unwrap()).toBe(source)
        expect(profile.findByName('epor').unwrap()).toBe(sink)
        expect(profile.findByName('missing').isNone()).toBe(true)
    })

    it('findByName prefers an exact match over a substring match', () => {
        const { profile, port } = makeProfile()
        // 'port' is a substring of 'report', but the exact match wins
        expect(profile.findByName('port').unwrap()).toBe(port)
    })

    it('propagates the name to ancestors for collapsed display', () => {
        const profile = new CircuitProfile(1, 'n')
        const outer = new ComplexNode('c1', 'region', 1)
        const inner = new ComplexNode('c2', 'subregion', 1)
        const source = new SimpleNode('n1', 'source', 1)
        profile.complexNodes.set(outer.id, outer)
        profile.complexNodes.set(inner.id, inner)
        profile.simpleNodes.set(source.id, source)
        profile.parents.set(inner.id, outer.id)
        profile.parents.set(source.id, inner.id)
        profile.byPersistentId.set('abc123', source)

        profile.setDataflow({ calcite_plan: {}, mir: { s1: mirNode('abc123', 'customers', null) } })

        expect(outer.collapsedOperation()).toBe('region customers')
        expect(inner.collapsedOperation()).toBe('subregion customers')
        // The expanded label stays unchanged
        expect(outer.operation).toBe('region')
    })
})
