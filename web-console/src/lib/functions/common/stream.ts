/**
 * Read from a stream, yielding one line at a time.
 * @see https://developer.mozilla.org/en-US/docs/Web/API/ReadableStreamDefaultReader/read#example_2_-_handling_text_line_by_line
 * @param response
 */
export async function* readLineFromStream(response: Response) {
  const utf8Decoder = new TextDecoder('utf-8')
  if (!response.body) {
    throw new Error('No body when fetching request.')
  }
  const reader = response.body.getReader()
  let { value: chunk, done: readerDone } = await reader.read()
  let decodedChunk = chunk ? utf8Decoder.decode(chunk, { stream: true }) : ''

  const re = /\r\n|\n|\r/gm
  let startIndex = 0

  for (;;) {
    const result = re.exec(decodedChunk)
    if (!result) {
      if (readerDone) {
        break
      }
      const remainder = decodedChunk.substring(startIndex)
      ;({ value: chunk, done: readerDone } = await reader.read())
      decodedChunk = remainder + (chunk ? utf8Decoder.decode(chunk, { stream: true }) : '')
      startIndex = re.lastIndex = 0
      continue
    }
    yield decodedChunk.substring(startIndex, result.index)
    startIndex = re.lastIndex
  }
  if (startIndex < decodedChunk.length) {
    // last line didn't end in a newline char
    yield decodedChunk.substring(startIndex)
  }
}
