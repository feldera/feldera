import { Dispatch, useState } from 'react'

import Card from '@mui/material/Card'
import { useQuery } from '@tanstack/react-query'
import { GridColumns, GridRenderCellParams } from '@mui/x-data-grid-pro'

import { ConnectorService } from 'src/types/manager/services/ConnectorService'
import { ConnectorDescr } from 'src/types/manager/models/ConnectorDescr'
import EntityTable from 'src/components/table/EntityTable'
import Button from '@mui/material/Button'
import { ConnectorType, Direction } from 'src/types/manager'

const SelectSourceTable = (props: {
  direction: Direction
  typ: ConnectorType
  onAddClick: Dispatch<ConnectorDescr>
}) => {
  const [rows, setRows] = useState<ConnectorDescr[]>([])
  const fetchQuery = useQuery({ queryKey: ['connector'], queryFn: ConnectorService.listConnectors })

  const columns: GridColumns = [
    {
      flex: 0.05,
      minWidth: 50,
      field: 'connector_id',
      headerName: 'ID'
    },
    {
      flex: 0.3,
      minWidth: 290,
      headerName: 'Name',
      field: 'name'
    },
    {
      flex: 0.15,
      minWidth: 140,
      field: 'typ',
      headerName: 'Type'
    },
    {
      flex: 0.1,
      minWidth: 90,
      sortable: false,
      field: 'actions',
      headerName: '',
      renderCell: (params: GridRenderCellParams) => {
        return (
          <Button size='small' variant='outlined' color='secondary' onClick={() => props.onAddClick(params.row)}>
            Add
          </Button>
        )
      }
    }
  ]

  const tableProps = {
    density: 'compact' as const,
    getRowId: (row: ConnectorDescr) => row.connector_id,
    columnVisibilityModel: { connector_id: false, typ: false },
    columns: columns,
    rows: rows.filter(row => row.direction === props.direction || row.direction === Direction.INPUT_OUTPUT),
    hideFooter: true,
    initialState: {
      filter: {
        filterModel: {
          items: [
            {
              id: 1,
              columnField: 'typ',
              operatorValue: 'equals',
              value: props.typ
            }
          ]
        }
      }
    }
  }

  return (
    <Card
      sx={{
        color: 'secondary.main',
        m: 5
      }}
    >
      <EntityTable
        tableProps={tableProps}
        setRows={setRows}
        fetchRows={fetchQuery}
        hasFilter={false}
        addActions={false}
      />
    </Card>
  )
}

export default SelectSourceTable
