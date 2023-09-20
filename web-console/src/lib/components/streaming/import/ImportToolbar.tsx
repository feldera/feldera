import useDefaultRows from '$lib/compositions/streaming/import/useDefaultRows'
import useGenerateRows from '$lib/compositions/streaming/import/useGenerateRows'
import useInsertRows from '$lib/compositions/streaming/import/useInsertRows'
import { Row } from '$lib/functions/ddl'
import { PipelineRevision, PipelineStatus, Relation } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { LS_PREFIX } from '$lib/types/localStorage'
import dayjs from 'dayjs'
import Papa from 'papaparse'
import { ChangeEvent, Dispatch, MutableRefObject, SetStateAction, useCallback } from 'react'

import { Icon } from '@iconify/react'
import { useLocalStorage } from '@mantine/hooks'
import { Button } from '@mui/material'
import {
  GridApi,
  GridToolbarColumnsButton,
  GridToolbarContainer,
  GridToolbarDensitySelector
} from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'

import RngSettingsDialog, { StoredFieldSettings } from './RngSettingsDialog'

export type StoreSettingsFn = (
  val:
    | Map<string, StoredFieldSettings>
    | ((prevState: Map<string, StoredFieldSettings>) => Map<string, StoredFieldSettings>)
) => void

const ImportToolbar = (props: {
  relation: Relation
  setRows: Dispatch<SetStateAction<any[]>>
  pipelineRevision: PipelineRevision
  setLoading: Dispatch<SetStateAction<boolean>>
  apiRef: MutableRefObject<GridApi>
  rows: Row[]
}) => {
  const { relation, setRows, pipelineRevision, setLoading, apiRef, rows } = props

  const { data: pipeline } = useQuery(PipelineManagerQuery.pipelineStatus(pipelineRevision.pipeline.pipeline_id))
  const isRunning = pipeline?.state.current_status === PipelineStatus.RUNNING

  const [settings, setSettings] = useLocalStorage<Map<string, StoredFieldSettings>>({
    key: [LS_PREFIX, 'rng-settings', pipelineRevision.program.program_id, relation.name].join('-'),
    serialize: value => {
      return JSON.stringify(Array.from(value.entries()))
    },
    deserialize: localStorageValue => {
      const parsed: [string, StoredFieldSettings][] = JSON.parse(localStorageValue)
      parsed.map(v => {
        v[1].config.time = dayjs(new Date(v[1].config.time))
        v[1].config.time2 = dayjs(new Date(v[1].config.time2))
        v[1].config.date_range = [
          dayjs(new Date(v[1].config.date_range[0])),
          dayjs(new Date(v[1].config.date_range[1]))
        ]
      })
      const deserializedMap = new Map(parsed)
      return deserializedMap
    },
    defaultValue: new Map()
  })

  const handleClearData = () => {
    setRows([])
  }

  const insertDefaultRows = useDefaultRows(apiRef, setRows, relation)
  const insertRandomRows = useGenerateRows(apiRef, setRows, relation, settings)

  // Function to handle the CSV file import
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const handleFileChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setLoading(true)
      if (event.target.files && event.target.files.length > 0) {
        const file = event.target.files[0]
        const reader = new FileReader()

        reader.onload = e => {
          if (e.target) {
            const content = e.target.result
            if (typeof content === 'string') {
              const { data: parsedData } = Papa.parse<Record<string, any>>(content.trimEnd(), { header: true })
              const newRows: Row[] = []
              parsedData.forEach((record, index) => {
                const row: Row = { genId: index, weight: 1, record }
                newRows.push(row)
              })
              setLoading(false)
              setRows(newRows)
            }
          }
        }
        reader.readAsText(file)
      }
    },
    [setLoading, setRows]
  )

  // Sends new rows to the table
  const insertRows = useInsertRows()
  const handleInsertRows = useCallback(() => {
    if (relation && rows.length > 0) {
      insertRows(pipelineRevision.pipeline.pipeline_id, relation, !isRunning, rows, setRows)
    }
  }, [insertRows, pipelineRevision, isRunning, relation, rows, setRows])

  return relation && pipelineRevision ? (
    <GridToolbarContainer>
      <GridToolbarColumnsButton />
      <GridToolbarDensitySelector />
      <Button size='small' onClick={handleClearData} startIcon={<Icon icon='mi:delete' />}>
        Clear
      </Button>
      <RngSettingsDialog relation={relation} settings={settings} setSettings={setSettings} />
      <Button size='small' onClick={() => insertDefaultRows(1)} startIcon={<Icon icon='majesticons:add-row' />}>
        Add Default Row
      </Button>
      <Button size='small' onClick={() => insertRandomRows(1)} startIcon={<Icon icon='mdi:add' />}>
        1 Random Row
      </Button>
      <Button size='small' onClick={() => insertRandomRows(5)} startIcon={<Icon icon='mdi:add-bold' />}>
        5 Random Rows
      </Button>
      <Button size='small' onClick={handleInsertRows} startIcon={<Icon icon='mdi:upload' />} color='info'>
        Insert Rows
      </Button>
    </GridToolbarContainer>
  ) : (
    <></>
  )
}

export default ImportToolbar
