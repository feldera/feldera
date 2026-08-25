// The SQL compiler and a text editor disagree about what the end of a source range means, and the
// diagram's "go to the code of this node" navigation crosses that boundary. `SourcePositionRange`
// owns both readings, so both are pinned here.

import { describe, expect, it } from 'vitest'

import { SourcePositionRange } from './dataflow.js'

// A line of real SQL with a real range from the compiler's dataflow graph: `fact_1` starts at column
// 14 and ends at column 19, which is the `1` - the last character of the name, not the space after it.
const LINE = 'CREATE TABLE fact_1 (id BIGINT);'
const TABLE_NAME = new SourcePositionRange({
    start_line_number: 1,
    start_column: 14,
    end_line_number: 1,
    end_column: 19
})

describe('source position ranges', () => {
    it('reports the last character of the range as its end, the way the compiler does', () => {
        // Calcite's `SqlParserPos.getEndColumnNum()`, which the compiler passes through verbatim, and
        // which its own error rendering slices as `line.substring(start.column - 1, end.column)`.
        expect(TABLE_NAME.end.column).toBe(19)
        expect(LINE.slice(TABLE_NAME.start.column - 1, TABLE_NAME.end.column)).toBe('fact_1')
    })

    it('offers an end an editor can select to, one past that character', () => {
        // Monaco and its kind treat a range end as exclusive: selecting to `end` would highlight
        // `fact_` and leave the `1` out, which is the bug this exists to prevent.
        expect(TABLE_NAME.endExclusive.line).toBe(TABLE_NAME.end.line)
        expect(TABLE_NAME.endExclusive.column).toBe(TABLE_NAME.end.column + 1)
        const exclusive = (line: string, r: SourcePositionRange) =>
            line.slice(r.start.column - 1, r.endExclusive.column - 1)
        expect(exclusive(LINE, TABLE_NAME)).toBe('fact_1')
        // A single character is a range whose two ends are the same column.
        const semicolon = new SourcePositionRange({
            start_line_number: 1,
            start_column: 32,
            end_line_number: 1,
            end_column: 32
        })
        expect(exclusive(LINE, semicolon)).toBe(';')
    })
})
