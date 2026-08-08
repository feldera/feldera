/** Build the download file name for an exported profile diagram: the pipeline name followed by
 *  the snapshot date as YYYY.MM.DD, e.g. "my-pipeline-2024.03.09.svg". Falls back to
 *  "dataflow-diagram" when no pipeline name is known.
 */
export function profileImageFileName(pipelineName: string | undefined, date: Date): string {
  const base = sanitizeFileNamePart(pipelineName ?? '') || 'dataflow-diagram'
  return `${base}-${formatDateYYYYMMDD(date)}.svg`
}

/** Format a date as zero-padded YYYY.MM.DD in local time. */
function formatDateYYYYMMDD(date: Date): string {
  const yyyy = String(date.getFullYear()).padStart(4, '0')
  const mm = String(date.getMonth() + 1).padStart(2, '0')
  const dd = String(date.getDate()).padStart(2, '0')
  return `${yyyy}.${mm}.${dd}`
}

/** Replace characters that are unsafe or awkward in file names with '-', collapse repeats, and
 *  trim leading/trailing separators. */
function sanitizeFileNamePart(value: string): string {
  return value
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^[-.]+|[-.]+$/g, '')
}
