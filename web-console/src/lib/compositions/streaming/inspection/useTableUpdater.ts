// Hook that fetches a set of rows for a table/view and then continously
// receives updates and applies them to the rows.
//
// In dbsp-speak this maintains an integral for a part of a relation.

import { readLineFromStream } from '$lib/functions/common/stream'
import { csvLineToRow, Row } from '$lib/functions/ddl'
import { NeighborhoodQuery, Relation } from '$lib/services/manager'
import { parse } from 'csv-parse'
import { Dispatch, SetStateAction, useCallback, useMemo, useState } from 'react'

/**
 * Mutate the oldRows to reflect the computed integral with all deltaRows
 * Idempotent
 * @param oldRows
 * @param deltaRows
 */
const updateRowsIntegral = (oldRows: Map<number, Row>, deltaRows: Row[]) => {
  for (const row of deltaRows) {
    const curRow = oldRows.get(row.genId)
    if (curRow && curRow.weight + row.weight > 0) {
      oldRows.set(row.genId, { ...row, weight: curRow.weight + row.weight })
    } else if (curRow && curRow.weight + row.weight <= 0) {
      oldRows.delete(row.genId)
    } else {
      oldRows.set(row.genId, row)
    }
  }
  return oldRows
}

const computeAndSet = (integral: Map<number, Row>) => (setRows: Dispatch<SetStateAction<Row[]>>, parsedRows: Row[]) => {
  setRows(old => {
    const relationChanged = !old.length
    if (relationChanged) {
      integral.clear()
    }
    updateRowsIntegral(integral, parsedRows)
    return Array.from(integral.values()).sort((a, b) => a.genId - b.genId)
  })
  return integral
}

const computeInBackground =
  (integral: Map<number, Row>) => (setRows: Dispatch<SetStateAction<Row[]>>, parsedRows: Row[]) => {
    setRows(old => {
      const relationChanged = !old.length
      if (relationChanged) {
        integral.clear()
      }
      updateRowsIntegral(integral, parsedRows)
      if (!relationChanged) {
        return old
      }
      return Array.from(integral.values()).sort((a, b) => a.genId - b.genId)
    })
    return integral
  }

type StateType = {
  rowsCallback: (setRows: Dispatch<SetStateAction<Row[]>>, parsedRows: Row[]) => Map<number, Row>
} & {
  [_ in 'pause' | 'resume']?: (setState: Dispatch<SetStateAction<StateType>>) => () => void
}

const paused: (prevState: StateType) => StateType = old => ({
  rowsCallback: computeInBackground(old.rowsCallback(() => {}, [])),
  resume: setState => () => setState(resumed)
})

const resumed: (prevState: StateType) => StateType = old => ({
  rowsCallback: computeAndSet(old.rowsCallback(() => {}, [])),
  pause: setState => () => setState(paused)
})

export function useTableUpdater() {
  const utf8Decoder = useMemo(() => new TextDecoder('utf-8'), [])
  const [{ rowsCallback, pause, resume }, setState] = useState(resumed({ rowsCallback: () => new Map() }))
  const readStream = useCallback(
    async (
      url: URL,
      requestedNeighborhood: NeighborhoodQuery,
      setRows: Dispatch<SetStateAction<Row[]>>,
      setLoading: Dispatch<SetStateAction<boolean>>,
      relation: Relation,
      controller: AbortController
    ) => {
      // We try and fetch one more row than requested,
      // this helps to detect if this is the last page.
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
          if (obj.text_data === undefined) {
            // A ping message, we ignore this.
            continue
          }

          parse(
            obj.text_data,
            {
              delimiter: ','
            },
            (error, result: string[][]) => {
              if (error) {
                console.error(error)
              }
              const parsedRows = result.map(row => csvLineToRow(relation, row))

              setLoading(false)
              rowsCallback(setRows, parsedRows)
            }
          )
        }
      } catch (e) {
        if (e instanceof TypeError) {
          if (e.message == 'Error in body stream' || e.message == 'Error in input stream') {
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
    [utf8Decoder, rowsCallback]
  )

  return {
    updateTable: readStream,
    pause: pause?.(setState),
    resume: resume?.(setState)
  }
}
