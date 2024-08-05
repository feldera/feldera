import type { XgressRecord } from '$lib/types/pipelineManager'
import JSONbig from 'true-json-bigint'

export const accumulateChanges = async (
  reader: ReadableStreamDefaultReader<Uint8Array>,
  pushChanges: (changes: Record<'insert' | 'delete', XgressRecord>[]) => void
) => {
  const decoder = new TextDecoder()
  let buffer = ''
  while (true) {
    const { done, value } = await reader.read() // .catch((e) => {
    // if (e instanceof DOMException && e.message === 'BodyStreamBuffer was aborted') {
    //   return {
    //     done: true,
    //     value: undefined
    //   }
    // }
    //   throw e
    // })
    if (done) {
      break
    }

    buffer += decoder.decode(value, { stream: true })

    let boundary = -1
    while ((boundary = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, boundary).trim()
      buffer = buffer.slice(boundary + 1)
      if (line === '') {
        continue
      }
      const obj: {
        sequence_number: number
        json_data?: Record<'insert' | 'delete', XgressRecord>[]
      } = JSONbig.parse(line)
      if (!obj.json_data) {
        continue
      }
      pushChanges(obj.json_data)
    }
  }
}
