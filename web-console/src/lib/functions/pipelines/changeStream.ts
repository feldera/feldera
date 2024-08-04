import type { XgressRecord } from '$lib/types/pipelineManager'
import JSONbig from 'true-json-bigint'

const decoder = new TextDecoder()

export const accumulateChanges = async (
  reader: ReadableStreamDefaultReader<Uint8Array>,
  pushChanges: (changes: Record<'insert' | 'delete', XgressRecord>[]) => void
) => {
  while (true) {
    const { done, value } = await reader.read().catch((e) => {
      // if (e instanceof DOMException && e.message === 'BodyStreamBuffer was aborted') {
      //   return {
      //     done: true,
      //     value: undefined
      //   }
      // }
      throw e
    })
    if (done) {
      break
    }

    const chunk = decoder.decode(value, { stream: true })
    const strings = chunk.split(/\r?\n/)

    for (const str of strings) {
      if (str.trim() === '') {
        continue // Add only non-empty strings
      }
      const obj: {
        sequence_number: number
        json_data?: Record<'insert' | 'delete', XgressRecord>[]
      } = JSONbig.parse(str)
      if (!obj.json_data) {
        continue
      }
      pushChanges(obj.json_data)
    }
  }
}
