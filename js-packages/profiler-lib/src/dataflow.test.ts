import { describe, expect, it } from 'vitest'
import { SourcePositionRange, SourcePositionRanges } from './dataflow.js'

const range = (line: number, column: number) => new SourcePositionRange(
    { start_line_number: line, start_column: column, end_line_number: line, end_column: column + 5 })
const lineAndColumn = (ranges: SourcePositionRanges) =>
    ranges.positions.map(p => [p.range.start_line_number, p.range.start_column])

describe('SourcePositionRanges.append', () => {
    it('adds the new ranges in their order and skips the ranges it has', () => {
        const ranges = new SourcePositionRanges([range(5, 1), range(2, 1)])
        ranges.append(new SourcePositionRanges([range(9, 1), range(2, 1), range(5, 3), range(9, 1)]))
        expect(lineAndColumn(ranges)).toEqual([[5, 1], [2, 1], [9, 1], [5, 3]])
    })

    it('still skips known ranges after sort removes duplicates', () => {
        const ranges = new SourcePositionRanges([range(4, 1), range(1, 1), range(4, 1)])
        ranges.append(new SourcePositionRanges([range(3, 1)]))
        ranges.sort()
        ranges.append(new SourcePositionRanges([range(4, 1), range(1, 1), range(2, 1)]))
        expect(lineAndColumn(ranges)).toEqual([[1, 1], [3, 1], [4, 1], [2, 1]])
    })

    it('gives the same result for a long list', () => {
        const lines = Array.from({ length: 40 }, (_, i) => 40 - i)
        const ranges = new SourcePositionRanges(lines.map(line => range(line, 1)))
        ranges.append(new SourcePositionRanges([range(50, 1), range(7, 1), range(50, 1), range(7, 2)]))
        ranges.sort()
        ranges.append(new SourcePositionRanges([range(60, 1), range(50, 1), range(60, 1), range(1, 1)]))
        expect(lineAndColumn(ranges)).toEqual([
            ...lines.slice().reverse().flatMap(line => line === 7 ? [[7, 1], [7, 2]] : [[line, 1]]),
            [50, 1], [60, 1]])
    })
})
