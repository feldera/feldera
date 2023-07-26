// Hook that fetches a set of rows for a table/view and then continously
// receives updates and applies them to the rows.
//
// In dbsp-speak this maintains an integral for a part of a relation.

import { Dispatch, SetStateAction, useCallback, useMemo } from 'react'
import { parse } from 'csv-parse'
import { NeighborhoodQuery, Relation } from 'src/types/manager'
import { parseSqlType } from 'src/types/ddl'
import { readLineFromStream } from 'src/utils'

function useTableUpdater() {
  const utf8Decoder = useMemo(() => new TextDecoder('utf-8'), [])
  const readStream = useCallback(
    async (
      url: URL,
      requestedNeighborhood: NeighborhoodQuery,
      setRows: Dispatch<SetStateAction<any[]>>,
      setLoading: Dispatch<SetStateAction<boolean>>,
      relation: Relation,
      controller: AbortController
    ) => {
      // We try and fetch one more row than requested, this helps to detect if
      // this is the last page.
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestedNeighborhood),
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
          const obj = JSON.parse(line)
          parse(
            obj.text_data,
            {
              delimiter: ','
            },
            (error, result: string[][]) => {
              if (error) {
                console.error(error)
              }

              // Convert a row of strings to an object of typed values this is
              // important because for sending a row as an anchor later it needs
              // to have proper types (a number can't be a string etc.)
              const typedRecords: any[] = result.map(row => {
                const genId = Number(row[0])
                const weight = row[row.length - 1]
                const fields = row.slice(1, row.length - 1)
                const newRow = { genId, weight: parseInt(weight) } as any
                relation.fields.forEach((col, i) => {
                  newRow[col.name] = parseSqlType(col, fields[i])
                })

                return newRow
              })

              setLoading(false)
              // We compute the integral and store it in rows
              setRows(curRows => {
                const rows = new Map()
                curRows.forEach(row => rows.set(row.genId, row))
                for (const row of typedRecords) {
                  const curRow = rows.get(row.genId)
                  if (curRow && curRow.weight + row.weight > 0) {
                    rows.set(row.genId, { ...row, weight: curRow.weight + row.weight })
                  } else if (curRow && curRow.weight + row.weight <= 0) {
                    rows.delete(row.genId)
                  } else {
                    rows.set(row.genId, row)
                  }
                }
                return Array.from(rows.values()).sort((a, b) => a.genId - b.genId)
              })
            }
          )
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

export default useTableUpdater
