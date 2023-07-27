import { ChangeEvent, useCallback, Dispatch, SetStateAction, MutableRefObject } from 'react'
import { Button } from '@mui/material'
import { Icon } from '@iconify/react'
import {
  GridApi,
  GridToolbarColumnsButton,
  GridToolbarContainer,
  GridToolbarDensitySelector
} from '@mui/x-data-grid-pro'
import Papa from 'papaparse'

import { PipelineRevision, Relation } from 'src/types/manager'
import { Row } from 'src/types/ddl'
import RngSettingsDialog, { StoredFieldSettings } from './RngSettingsDialog'
import { LS_PREFIX } from 'src/types/localStorage'
import { useLocalStorage } from '@mantine/hooks'
import useGenerateRows from './hooks/useGenerateRows'
import useInsertRows from './hooks/useInsertRows'
import dayjs from 'dayjs'

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

  // Handler function to clear the rows
  const handleClearData = () => {
    setRows([])
  }

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
    if (relation) {
      insertRows(pipelineRevision.pipeline.pipeline_id, relation, rows)
    }
  }, [insertRows, pipelineRevision, relation, rows])

  return relation && pipelineRevision ? (
    <GridToolbarContainer>
      <GridToolbarColumnsButton />
      <GridToolbarDensitySelector />
      <Button size='small' onClick={handleClearData} startIcon={<Icon icon='mi:delete' />}>
        Clear
      </Button>
      <RngSettingsDialog relation={relation} settings={settings} setSettings={setSettings} />
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
