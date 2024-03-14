// Hook that fetches the quantiles for a relation.
//
// The reason this is not using the standard useQuery hook is that quantiles
// come as part of a HTTP stream and our openapi client generator does not
// support streaming yet.

import { readLineFromStream } from '$lib/functions/common/stream'
import { parseCSVValue, SQLValueJS } from '$lib/functions/ddl'
import { getUrl, httpOutputOptions } from '$lib/services/HttpInputOutputService'
import { Chunk, HttpInputOutputService, OpenAPI, Relation } from '$lib/services/manager'
import { getHeaders } from '$lib/services/manager/core/request'
import { Arguments } from '$lib/types/common/function'
import { parse } from 'csv-parse'
import { Dispatch, SetStateAction, useCallback, useMemo } from 'react'

function useQuantiles() {
  const utf8Decoder = useMemo(() => new TextDecoder('utf-8'), [])
  const readStream = useCallback(
    async (
      egressParams: Arguments<typeof HttpInputOutputService.httpOutput>,
      setQuantiles: Dispatch<SetStateAction<SQLValueJS[][] | undefined>>,
      relation: Relation,
      controller: AbortController
    ) => {
      // TODO:
      //    The following uses some of the code generated from OpenAPI to enable request authentication when it is configured
      //    This needs to be eventually refactored away, probably in favor of HttpInputOutputService.httpOutput(...egressParams)
      const options = httpOutputOptions(...egressParams)
      const url = await getUrl(OpenAPI, options)
      const headers = await getHeaders(OpenAPI, options)
      const response = await fetch(url, {
        method: options.method,
        headers,
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
                const typedRecords = result.map(row => {
                  const fields = row
                  const newRow = [] as SQLValueJS[]
                  relation.fields.forEach((col, i) => {
                    newRow[i] = parseCSVValue(col.columntype, fields[i])
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
