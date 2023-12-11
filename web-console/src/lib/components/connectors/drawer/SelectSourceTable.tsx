// The table that pops up when the user clicks on a connector type in the drawer
// to select from an existing list of connectors.

import EntityTable from '$lib/components/common/table/EntityTable'
import { connectorDescrToType } from '$lib/functions/connectors'
import { ConnectorDescr } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import Link from 'next/link'
import { Dispatch, useState } from 'react'
import IconShow from '~icons/bx/show'

import { Box, IconButton, Tooltip } from '@mui/material'
import Button from '@mui/material/Button'
import Card from '@mui/material/Card'
import { GridColDef, GridRenderCellParams } from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'

import type { ConnectorType, Direction } from '$lib/types/connectors'
const SelectSourceTable = (props: {
  direction: Direction
  typ: ConnectorType
  onAddClick: Dispatch<ConnectorDescr>
}) => {
  const [rows, setRows] = useState<ConnectorDescr[]>([])
  const fetchQuery = useQuery(PipelineManagerQuery.connector())

  const columns: GridColDef[] = [
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
      width: 120,
      sortable: false,
      field: 'actions',
      headerName: '',
      renderCell: (params: GridRenderCellParams<ConnectorDescr>) => {
        return (
          <Box sx={{ display: 'flex', gap: 2 }}>
            <Button size='small' variant='outlined' color='secondary' onClick={() => props.onAddClick(params.row)}>
              Add
            </Button>
            <Tooltip title='Inspect' key='inspect'>
              <IconButton size='small' component={Link} href={'#view/connector/' + params.row.connector_id}>
                <IconShow fontSize={20} />
              </IconButton>
            </Tooltip>
          </Box>
        )
      }
    }
  ]

  const tableProps = {
    density: 'compact' as const,
    getRowId: (row: ConnectorDescr) => row.connector_id,
    columnVisibilityModel: { connector_id: false },
    columns: columns,
    rows: rows.filter(row => {
      return connectorDescrToType(row) == props.typ
    }),
    hideFooter: true
  }
  return (
    <Card
      sx={{
        color: 'secondary.main',
        m: 5
      }}
    >
      <EntityTable tableProps={tableProps} setRows={setRows} fetchRows={fetchQuery} hasFilter={false} />
    </Card>
  )
}

export default SelectSourceTable
