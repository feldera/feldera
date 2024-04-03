// Hook that fetches a set of rows for a table/view and then continously
// receives updates and applies them to the rows.
//
// In dbsp-speak this maintains an integral for a part of a relation.

import { readLineFromStream } from '$lib/functions/common/stream'
import { JSONXgressValue, Row, xgressJSONToSQLRecord } from '$lib/functions/sqlValue'
import { getUrl, httpOutputOptions } from '$lib/services/HttpInputOutputService'
import { HttpInputOutputService, OpenAPI, Relation } from '$lib/services/manager'
import { getHeaders } from '$lib/services/manager/core/request'
import { Arguments } from '$lib/types/common/function'
import { Dispatch, SetStateAction, useCallback, useMemo, useState } from 'react'
import JSONbig from 'true-json-bigint'

/**
 * Mutate the oldRows to reflect the computed integral with all deltaRows
 * Idempotent
 * @param oldRows
 * @param deltaRows
 */
const updateRowsIntegral = (oldRows: Map<number, Row>, deltaRows: Row[]) => {
  for (const row of deltaRows) {
    const curRow = oldRows.get(row.genId)
    const weight = curRow ? curRow.weight + row.weight : null
    if (weight === null) {
      oldRows.set(row.genId, row)
    } else if (weight > 0) {
      oldRows.set(row.genId, { ...row, weight })
    } else if (weight <= 0) {
      oldRows.delete(row.genId)
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
      egressParams: Arguments<typeof HttpInputOutputService.httpOutput>,
      setRows: Dispatch<SetStateAction<Row[]>>,
      setLoading: Dispatch<SetStateAction<boolean>>,
      relation: Relation,
      controller: AbortController
    ) => {
      // We try and fetch one more row than requested,
      // this helps to detect if this is the last page.
      // TODO:
      //    The following uses some of the code generated from OpenAPI to enable request authentication when it is configured
      //    This needs to be eventually refactored away, probably in favor of HttpInputOutputService.httpOutput(...egressParams)
      const options = httpOutputOptions(...egressParams)
      const url = await getUrl(OpenAPI, options)
      const headers = await getHeaders(OpenAPI, options)
      const response = await fetch(url, {
        method: options.method,
        headers,
        body: JSONbig.stringify(options.body),
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
          const obj = JSONbig.parse(line)

          setLoading(false)

          if (!obj.json_data) {
            // A ping message, we ignore this.
            continue
          }
          const parsedRows = (obj.json_data as any[]).map(item =>
            (([action, row]) => xgressJSONToSQLRow(relation, row as any, action === 'insert' ? 1 : -1))(
              Object.entries(item)[0]
            )
          )

          rowsCallback(setRows, parsedRows)
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

const xgressJSONToSQLRow = (
  relation: Relation,
  entry: { key: Record<string, JSONXgressValue>; index: number },
  weight: number
): Row => ({
  genId: entry.index,
  weight,
  record: xgressJSONToSQLRecord(relation, entry.key)
})
