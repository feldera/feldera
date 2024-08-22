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
  const reader = stream.pipeThrough(splitByNewlineTransformStream()).getReader() // chunkStreamByNewline(stream).getReader()
  let count = 0
  let resultBuffer = new Array()

  const onValue = ({ value }: ParsedElementInfo) => {
    // console.log('onValue')
    resultBuffer[count] = value
    ++count
  }

  const mkParser = (): { write: JSONParser['write']; end: JSONParser['end'] } => {
    // const parser = new JSONParser({ paths: ['$.json_data.*'] })
    // parser.onValue = onValue
    // return parser
    const tokenizer = new BigNumberTokenizer()
    const tokenParser = new TokenParser({ paths: ['$.json_data.*'] })
    tokenizer.onToken = tokenParser.write.bind(tokenParser)
    tokenParser.onValue = onValue
    return tokenizer
  }

  let parser = mkParser()

  const chunksToParse = [] as Uint8Array[]

  const startInterruptableParse = () => {
    let end = false
    let done = true
    return new Promise<{interrupt: () => void, stop: () => void}>(async (resolve) => {
      const interrupt = async () => {
        try {
          parser.end()
          // console.log('ended json')
        } catch {
          // We ignore the error because we just want to interrupt parsing and production of values
          // console.log('interrupted')
        }
        chunksToParse.length = 0
        while (!done) {
          // Release thread to parse JSON
          await new Promise((resolve) => setTimeout(resolve))
        }
        parser = mkParser()
      }
      resolve({interrupt, stop () { end = true; interrupt() }})

      while (!end) {
        const value = chunksToParse.shift()
        if (!value) {
          await new Promise((resolve) => setTimeout(resolve))
          continue
        }
        const positions = generateArray(0, value.length, 200000)
        const pairs = discreteDerivative(positions, tuple)
        for (const [n1, n0] of pairs) {
          count = 0
          done = false
          try {
            parser.write(value.slice(n0, n1))
          } catch (e) {
            console.log('parse error!', e)
            done = true
            break
          }

          pushChanges(resultBuffer.slice(0, count))
          done = true
          await new Promise((resolve) => setTimeout(resolve))
        }
        // commit(count)
      }

    })
  }

  setTimeout(async () => {
    const {interrupt, stop} = await startInterruptableParse()
    while (true) {
      const { done, value } = await reader.read()
      if (done || !value) {
        stop()
        break
      }
      // console.log('cycle!', value.length)
      if (value.length === 0) {
        interrupt()
        continue
      }
      chunksToParse.push(value)
      // Release thread to process UI
      await new Promise((resolve) => setTimeout(resolve))
    }
  })
  return () => reader.cancel()
}

function splitByNewlineTransformStream(): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
          let start = 0;
          console.log('upstream')

          while (start < chunk.length) {
              const newlineIndex = chunk.indexOf(10, start);
              const end = (newlineIndex === -1) ? chunk.length : newlineIndex

              // Enqueue the subarray, including the newline character if found
              controller.enqueue(chunk.subarray(start, end))
              if (end !== chunk.length) {
                console.log('yield end', end, chunk.length)
                controller.enqueue(new Uint8Array())
              }

              // Move start to after the newline character
              start = end + 1
          }
      },
      flush(controller) {
        controller.enqueue(new Uint8Array())
      }
  });
}