// Hook that fetches the quantiles for a relation.
//
// The reason this is not using the standard useQuery hook is that quantiles
// come as part of a HTTP stream and our openapi client generator does not
// support streaming yet.

import { readLineFromStream } from '$lib/functions/common/stream'
import { parseValueSafe } from '$lib/types/ddl'
import { Chunk, Relation } from '$lib/types/manager'
import { parse } from 'csv-parse'
import { Dispatch, SetStateAction, useCallback, useMemo } from 'react'

function useQuantiles() {
  const utf8Decoder = useMemo(() => new TextDecoder('utf-8'), [])
  const readStream = useCallback(
    async (
      url: URL,
      setQuantiles: Dispatch<SetStateAction<any[][] | undefined>>,
      relation: Relation,
      controller: AbortController
    ) => {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        signal: controller.signal
      }).catch(error => {
        return Promise.reject(error)
      })
      if (!response.ok) {
        if (!response.body) {
          throw new Error('Invalid error response from server: no body.')
        }
        const reader = response.body?.getReader()
        const { value: chunk } = await reader.read()
        const decodedChunk = chunk
          ? utf8Decoder.decode(chunk, { stream: false })
          : '{ "message": "Unable to decode server error: Invalid UTF-8 string.", "error_code": "UIInvalidUtf8" }'
        try {
          const error = JSON.parse(decodedChunk)
          return Promise.reject(error)
        } catch (e) {
          if (e instanceof SyntaxError) {
            throw new Error('Received invalid error format from server: ' + decodedChunk)
          } else {
            throw e
          }
        }
      }

      try {
        for await (const line of readLineFromStream(response)) {
          const obj: Chunk = JSON.parse(line)
          if (obj.text_data) {
            parse(
              obj.text_data,
              {
                delimiter: ','
              },
              (error, result: string[][]) => {
                if (error) {
                  console.error(error)
                }
                // Convert a row of strings to a typed record. This is important
                // because for sending a row as an anchor later it needs to have
                // proper types (a number can't be a string etc.)
                const typedRecords: any[][] = result.map(row => {
                  const fields = row
                  const newRow = [] as any
                  relation.fields.forEach((col, i) => {
                    newRow[i] = parseValueSafe(col.columntype, fields[i])
                  })
                  return newRow
                })

                setQuantiles(typedRecords)
              }
            )
          }
        }
      } catch (e) {
        if (e instanceof TypeError) {
          if (e.message == 'Error in body stream') {
            // Stream got closed (e.g., navigate away from page, reload), don't
            // throw this.
          } else {
            throw e
          }
        } else {
          throw e
        }
      }
    },
    [utf8Decoder]
  )

  return readStream
}

export default useQuantiles
