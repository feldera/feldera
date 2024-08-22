import type { XgressRecord } from '$lib/types/pipelineManager'
import BigNumber from 'bignumber.js'
import { JSONParser, Tokenizer, TokenParser } from '@streamparser/json'
import type { ParsedElementInfo } from '@streamparser/json/utils/types/parsedElementInfo.js'
import JSONbig from 'true-json-bigint'
import { discreteDerivative } from '../common/math'
import { tuple } from '../common/tuple'

export const accumulateChangesBatch = async (
  reader: ReadableStreamDefaultReader<Uint8Array>,
  pushChanges: (changes: Record<'insert' | 'delete', XgressRecord>[]) => void
) => {
  const decoder = new TextDecoder()
  let buffer = ''
  /// ===
  const busyMs = 5
  let allowedUntilMs = 0
  /// ===
  while (true) {
    const { done, value } = await reader.read()
    if (done) {
      break
    }

    /// ===
    allowedUntilMs = Date.now() + busyMs
    /// ===

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

      /// ===
      if (allowedUntilMs < Date.now()) {
        const lastIdx = buffer.lastIndexOf('\n')
        buffer = buffer.slice(lastIdx === -1 ? 0 : lastIdx)
        break
      }
      /// ===
    }
  }
}

function generateArray(min: number, max: number, distance: number): number[] {
  const result: number[] = []
  // Start at min and go up to max, adding the distance each time
  for (let i = min; i < max; i += distance) {
    result.push(i)
  }
  result.push(max)
  return result
}

class BigNumberTokenizer extends Tokenizer {
  parseNumber(numberStr: string) {
    return BigNumber(numberStr) as any
  }
}

export const accumulateChangesSingular = (
  stream: ReadableStream<Uint8Array>,
  pushChanges: (changes: Record<'insert' | 'delete', XgressRecord>[]) => void
  // pushChange: (change: Record<'insert' | 'delete', XgressRecord>) => void,
  // commit: (batchSize: number) => void
) => {
  const reader = chunkStreamByNewline(stream).getReader()
  let count = 0
  let resultBuffer = new Array()

  const onValue = ({ value }: ParsedElementInfo) => {
    // console.log('onValue')
    resultBuffer[count] = value
    ++count
  }

  const mkParser = (): { write: JSONParser['write'], end: JSONParser['end'] } => {
    // const parser = new JSONParser({ paths: ['$.json_data.*'] })
    // parser.onValue = onValue
    // return parser
    const tokenizer = new BigNumberTokenizer()
    const tokenParser = new TokenParser({ paths: ['$.json_data.*'] })
    tokenizer.onToken = tokenParser.write.bind(tokenParser);
    tokenParser.onValue = onValue
    return tokenizer
  }

  let parser = mkParser()

  const startInterruptableParse = (value: Uint8Array) => {
    // let shouldStop = false
    let done = true
    return new Promise<Disposable>(async (resolve) => {
      const dispose = async () => {
        try {
          parser.end()
        } catch {
          // We ignore the error because we just want to interrupt parsing and production of values
          console.log('interrupted')
        }
        while (!done) {
          await new Promise((resolve) => setTimeout(resolve))
        }
        parser = mkParser()
      }
      resolve({
        [Symbol.dispose]: dispose
      })
      console.log('startInterruptableParse')

      const positions = generateArray(0, value.length, 200000)
      const pairs = discreteDerivative(positions, tuple)
      for (const [n1, n0] of pairs) {
        count = 0
        if (!value) {
          break
        }
        done = false
        try {
          // const a = Date.now()
          parser.write(value.slice(n0, n1))
          console.log('batch ok')
          // console.log('took ms', Date.now() - a)
        } catch (e) {
          // error = true
          // console.log('parse error!', e, new TextDecoder().decode(value.slice(0, n1)))
          break
        }
        pushChanges(resultBuffer.slice(0, count))
        done = true
        await new Promise((resolve) => setTimeout(resolve))
      }
      // commit(count)
      done = true
    })
  }

  let handle = {
    [Symbol.dispose]() {}
  }

  setTimeout(async () => {
    while (true) {
      const { done, value } = await reader.read()
      // console.log('cycle!', new TextDecoder().decode(value))
      handle[Symbol.dispose]()
      // console.log('disposed!')
      if (done || !value) {
        //console.log('stream end')
        break
      }
      // console.log('received', new TextDecoder().decode(value).slice(0, 20))
      handle = await startInterruptableParse(value)
      // await startInterruptableParse(value)
    }
  })
  return () => reader.cancel()
}

async function* transformStream(
  reader: ReadableStreamDefaultReader<Uint8Array>
): AsyncGenerator<Uint8Array> {
  let buffer = new Uint8Array(0)

  try {
    while (true) {
      const { value, done } = await reader.read()
      if (done) {
        if (buffer.length > 0) {
          yield buffer
        }
        break
      }

      let start = 0
      for (let i = 0; i < value.length; i++) {
        if (value[i] === 10) {
          // Newline character
          const chunk = value.subarray(start, i)
          if (buffer.length > 0) {
            const combined = new Uint8Array(buffer.length + chunk.length)
            combined.set(buffer)
            combined.set(chunk, buffer.length)
            yield combined
            buffer = new Uint8Array(0)
          } else {
            yield chunk
          }
          start = i + 1
        }
      }

      if (start < value.length) {
        const remaining = value.subarray(start)
        const newBuffer = new Uint8Array(buffer.length + remaining.length)
        newBuffer.set(buffer)
        newBuffer.set(remaining, buffer.length)
        buffer = newBuffer
      }
    }
  } finally {
    reader.releaseLock()
  }
}

function chunkStreamByNewline(stream: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
  let reader: ReadableStreamDefaultReader<Uint8Array>

  return new ReadableStream<Uint8Array>({
    start(controller) {
      reader = stream.getReader()

      ;(async () => {
        try {
          for await (const chunk of transformStream(reader)) {
            controller.enqueue(chunk)
          }
          controller.close()
        } catch (err) {
          controller.error(err)
        }
      })()
    },
    cancel(reason) {
      reader.cancel(reason) // Cancel the upstream stream
    }
  })
}
