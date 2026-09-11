/**
 * Groups log lines into fixed-size chunks for virtual rendering.
 *
 * Chunks are the unit the virtualiser mounts and unmounts, rather than individual lines. One
 * chunk means one absolutely positioned node and one resize observation instead of fifty, and it
 * shrinks the pinned-for-selection set by the same factor. GitHub's Actions log viewer settled on
 * the same size for the same reason.
 */

export const CHUNK_SIZE = 50

export type LogChunk = {
  /**
   * Identity of the chunk, derived from absolute line numbers rather than array position, so it
   * survives both appends at the end and evictions from the front.
   */
  key: number
  /** Index into the source `lines` array of this chunk's first line. */
  startLine: number
  lines: string[]
}

/**
 * Cut `lines` into chunks on fixed absolute-line boundaries.
 *
 * Boundaries are computed from `firstLineIndex + i`, not from `i`, so a chunk keeps its key as the
 * list grows. Only the trailing partial chunk is rebuilt on append, which is what lets the
 * virtualiser keep its measurements for everything above. When a streaming buffer evicts from the
 * front the leading chunk becomes partial and is re-measured; every chunk after it is untouched.
 */
export const chunkLines = (lines: readonly string[], firstLineIndex = 0): LogChunk[] => {
  const chunks: LogChunk[] = []
  let start = 0
  while (start < lines.length) {
    const key = Math.floor((firstLineIndex + start) / CHUNK_SIZE)
    const end = Math.min((key + 1) * CHUNK_SIZE - firstLineIndex, lines.length)
    chunks.push({ key, startLine: start, lines: lines.slice(start, end) })
    start = end
  }
  return chunks
}

/**
 * Position in `chunks` of the chunk holding `line`, or -1 when no chunk does.
 *
 * Arithmetic rather than a search. Chunks are cut on fixed absolute-line boundaries, so only the
 * first can be partial: everything after it is exactly {@link CHUNK_SIZE} lines long, which places
 * any line by division.
 */
export const chunkIndexOfLine = (chunks: readonly LogChunk[], line: number): number => {
  const head = chunks[0]?.lines.length
  if (head === undefined || line < 0) {
    return -1
  }
  const index = line < head ? 0 : 1 + Math.floor((line - head) / CHUNK_SIZE)
  const chunk = chunks[index]
  // The trailing chunk can be partial too, so a line past the end divides into a chunk that exists
  // but does not hold it.
  return chunk && line < chunk.startLine + chunk.lines.length ? index : -1
}
