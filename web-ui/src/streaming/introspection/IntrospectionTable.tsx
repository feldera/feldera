// Browse the contents of a table or a view.

import Card from '@mui/material/Card'
import { DataGridPro, GridColumns, useGridApiRef } from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { PipelineDescr, PipelineStatus, OpenAPI, PipelineRevision } from 'src/types/manager'
import { parse } from 'csv-parse'

export type IntrospectionTableProps = {
  pipelineDescr: PipelineDescr | undefined
  name: string | undefined
}

export const IntrospectionTable = ({ pipelineDescr, name }: IntrospectionTableProps) => {
  const apiRef = useGridApiRef()
  const [headers, setHeaders] = useState<GridColumns | undefined>(undefined)

  const pipelineRevisionQuery = useQuery<PipelineRevision>(
    ['pipelineLastRevision', { pipeline_id: pipelineDescr?.pipeline_id }],
    {
      enabled: pipelineDescr !== undefined && pipelineDescr.program_id !== undefined
    }
  )
  useEffect(() => {
    if (!pipelineRevisionQuery.isLoading && !pipelineRevisionQuery.isError && name) {
      if (pipelineRevisionQuery.data && pipelineRevisionQuery.data.program.schema) {
        const pipelineRevision = pipelineRevisionQuery.data
        const program = pipelineRevision.program
        const tables = program.schema?.inputs.find(v => v.name === name)
        const views = program.schema?.outputs.find(v => v.name === name)
        const relation = tables || views // name is unique in the schema

        if (relation) {
          const id = [{ field: 'genId', headerName: 'genId' }]
          setHeaders(
            id
              .concat(
                relation.fields.map((col: any) => {
                  return { field: col.name, headerName: col.name, flex: 1 }
                })
              )
              .concat([{ field: 'weight', headerName: 'weight' }])
          )
        }
      }
    }
  }, [pipelineRevisionQuery.isLoading, pipelineRevisionQuery.isError, pipelineRevisionQuery.data, setHeaders, name])

  // Stream changes from backend and update the table
  useEffect(() => {
    if (
      pipelineDescr &&
      pipelineDescr.status == PipelineStatus.RUNNING &&
      name !== undefined &&
      headers !== undefined &&
      apiRef.current
    ) {
      const watchStream = async function (url: string) {
        const response = await fetch(url)
        const reader = response.body?.getReader()
        const decoder = new TextDecoder('utf-8')

        while (true) {
          const value = await reader?.read().then(function (result) {
            return decoder.decode(result.value)
          })
          const obj = JSON.parse(value || '{}')
          parse(
            obj.text_data,
            {
              delimiter: ','
            },
            (error, result: string[][]) => {
              if (error) {
                console.error(error)
              }

              const newRows: any[] = result.map(row => {
                const genId = row[0]
                const weight = row[row.length - 1]
                const fields = row.slice(0, row.length - 1)

                const newRow = { genId, weight: parseInt(weight) } as any
                headers.forEach((col, i) => {
                  if (col.field !== 'genId' && col.field !== 'weight') {
                    newRow[col.field] = fields[i - 1]
                  }
                })

                return newRow
              })

              apiRef.current?.updateRows(
                newRows
                  .map(row => {
                    const curRow = apiRef.current.getRow(row.genId)
                    if (curRow !== null && curRow.weight + row.weight == 0) {
                      return row
                    } else if (curRow == null && row.weight < 0) {
                      return null
                    } else {
                      return { ...row, weight: row.weight + (curRow?.weight || 0) }
                    }
                  })
                  .filter(x => x !== null)
              )
            }
          )
        }
      }

      const url = OpenAPI.BASE + '/v0/pipelines/' + pipelineDescr.pipeline_id + '/egress/' + name + '?format=csv'
      watchStream(url)
    }
  }, [pipelineDescr, name, apiRef, headers])

  return (
    <Card>
      <DataGridPro
        columnVisibilityModel={{ genId: false, weight: false }}
        getRowId={(row: any) => row.genId}
        apiRef={apiRef}
        autoHeight
        columns={headers || []}
        loading={headers === undefined}
        rowThreshold={0}
        rows={[]}
      />
    </Card>
  )
}
