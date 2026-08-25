/**
 * Chunking and the line-to-chunk lookup behind `LogView`.
 *
 * `chunkIndexOfLine` places a line by arithmetic rather than by searching, which is only sound
 * because of how `chunkLines` cuts: on absolute-line boundaries, so at most the first and last
 * chunks are partial. The lookup is checked against a linear scan over every shape those two
 * partials can take — an eviction from the front moves the head boundary, an append moves the tail.
 */

import { describe, expect, it } from 'vitest'
import { CHUNK_SIZE, chunkIndexOfLine, chunkLines, type LogChunk } from '$lib/logChunks'

const linesOf = (count: number) => Array.from({ length: count }, (_, i) => `line ${i}`)

/** What the lookup has to agree with: the definition, walked. */
const scanForLine = (chunks: readonly LogChunk[], line: number) =>
  chunks.findIndex(
    (chunk) => line >= chunk.startLine && line < chunk.startLine + chunk.lines.length
  )

describe('chunkLines', () => {
  it('cuts on absolute boundaries, so a chunk keeps its key as the log grows', () => {
    const first = chunkLines(linesOf(60), 0)
    const grown = chunkLines(linesOf(140), 0)
    expect(first.map((c) => c.key)).toEqual([0, 1])
    expect(grown.map((c) => c.key)).toEqual([0, 1, 2])
    // The full leading chunk is byte-identical, which is what lets the virtualiser keep its
    // measurements for everything above the append.
    expect(grown[0].lines).toEqual(first[0].lines)
  })

  it('makes the leading chunk partial when the buffer has evicted from the front', () => {
    // Absolute lines 30..149, so the first chunk holds the tail of key 0 and the rest are whole.
    const chunks = chunkLines(linesOf(120), 30)
    expect(chunks.map((c) => c.key)).toEqual([0, 1, 2])
    expect(chunks.map((c) => c.lines.length)).toEqual([20, CHUNK_SIZE, CHUNK_SIZE])
    expect(chunks.map((c) => c.startLine)).toEqual([0, 20, 70])
  })

  it('yields nothing for an empty log', () => {
    expect(chunkLines([], 0)).toEqual([])
  })
})

describe('chunkIndexOfLine', () => {
  it('agrees with a linear scan for every line, at every head and tail offset', () => {
    // Head offsets sweep a full chunk, so the leading partial takes every possible width; lengths
    // straddle three boundaries so the trailing partial does too.
    for (const firstLineIndex of [0, 1, 7, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 13, 501]) {
      for (const count of [1, 2, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1, 137, 200]) {
        const chunks = chunkLines(linesOf(count), firstLineIndex)
        for (let line = 0; line < count; line++) {
          expect(
            chunkIndexOfLine(chunks, line),
            `firstLineIndex=${firstLineIndex} count=${count} line=${line}`
          ).toBe(scanForLine(chunks, line))
        }
      }
    }
  })

  it('reports -1 for a line past the end, including one inside the trailing chunk', () => {
    // 60 lines is a full chunk plus ten. Line 70 divides into chunk 1, which exists but stops at
    // line 60 — the case a bare division gets wrong.
    const chunks = chunkLines(linesOf(60), 0)
    expect(chunkIndexOfLine(chunks, 59)).toBe(1)
    expect(chunkIndexOfLine(chunks, 60)).toBe(-1)
    expect(chunkIndexOfLine(chunks, 70)).toBe(-1)
    expect(chunkIndexOfLine(chunks, 10_000)).toBe(-1)
  })

  it('reports -1 for a negative line and for an empty log', () => {
    expect(chunkIndexOfLine(chunkLines(linesOf(60), 0), -1)).toBe(-1)
    expect(chunkIndexOfLine([], 0)).toBe(-1)
  })
})
