import type { SQLValueJS } from '$lib/types/sql'
import type { DataType, Field, RecordBatch, TypeMap } from 'apache-arrow'
import { Type } from 'apache-arrow'
import BigNumber from 'bignumber.js'
import Dayjs from 'dayjs'

const arrowIpcValueToJS = <T extends DataType<Type, any>>(arrowType: Field<T>, value: any) => {
  switch (arrowType.typeId) {
    case Type.Int64: {
      return new BigNumber(value)
    }
    case Type.Uint64: {
      return new BigNumber(value)
    }
    case Type.Timestamp: {
      return Dayjs(value)
    }
    case Type.Date: {
      return Dayjs(value)
    }
    case Type.DateMillisecond: {
      return Dayjs(value)
    }
    default: {
      return value
    }
  }
}

export const arrowIpcBatchToJS = <T extends TypeMap>(
  batch: RecordBatch<T>
): { row: SQLValueJS[] }[] => {
  const res: { row: SQLValueJS[] }[] = new Array(batch.numRows)

  for (let i = 0; i < batch.numRows; i++) {
    const row: SQLValueJS[] = []

    for (let j = 0; j < batch.schema.fields.length; j++) {
      const field = batch.schema.fields[j]
      const column = batch.getChildAt(j)

      if (column && column.isValid(i)) {
        const value = column.get(i)
        row[j] = arrowIpcValueToJS(field, value)
      }
    }

    res[i] = { row }
  }
  return res
}
