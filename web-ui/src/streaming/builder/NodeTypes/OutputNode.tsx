// OutputNodes are displayed on the right and connected with views of the
// program.

import CardHeader from '@mui/material/CardHeader'
import Avatar from '@mui/material/Avatar'
import { Position, NodeProps, Connection, useReactFlow } from 'reactflow'
import { Box } from '@mui/material'
import IconButton from '@mui/material/IconButton'

import { Icon } from '@iconify/react'
import { Handle, Node } from '../NodeTypes'
import useNodeDelete from '../hooks/useNodeDelete'
import { connectorTypeToIcon } from 'src/types/connectors'
import { useQuery } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { ConnectorDescr, ConnectorType } from 'src/types/manager'

const OutputNode = ({ id, data }: NodeProps) => {
  const { getNode } = useReactFlow()
  const onDelete = useNodeDelete(id)

  // Fetch the connector data for the corresponding ac.connector_id
  const [connector, setConnector] = useState<ConnectorDescr | undefined>(undefined)
  const connectorQuery = useQuery<ConnectorDescr>(['connectorStatus', { connector_id: data.ac.connector_id }])
  useEffect(() => {
    if (!connectorQuery.isError && !connectorQuery.isLoading) {
      setConnector(connectorQuery.data)
    }
  }, [data, connectorQuery.isError, connectorQuery.isLoading, connectorQuery.data])

  // Only allow the connection if we're coming from a view
  const isValidConnection = (connection: Connection) => {
    if (connection.source) {
      const sourceNode = getNode(connection.source)

      return (
        sourceNode !== undefined &&
        sourceNode.type === 'sqlProgram' &&
        connection.sourceHandle != null &&
        connection.sourceHandle.startsWith('view-')
      )
    } else {
      return false
    }
  }

  return (
    <Node>
      <CardHeader
        title={connector?.name || '<unname>'}
        subheader={connector?.description || ''}
        sx={{ py: 5, alignItems: 'flex-start' }}
        titleTypographyProps={{ variant: 'h5' }}
        subheaderTypographyProps={{ variant: 'body1', sx: { color: 'text.disabled' } }}
        avatar={
          <Avatar sx={{ mt: 1.5, width: 42, height: 42 }}>
            <Icon icon={connectorTypeToIcon(connector?.typ || ConnectorType.FILE)} />
          </Avatar>
        }
        action={
          <Box sx={{ display: 'flex', alignItems: 'center', mt: -4, mr: -4 }} className='nodrag nopan'>
            <IconButton size='small' aria-label='close' sx={{ color: 'text.secondary' }} onClick={() => onDelete()}>
              <Icon icon='bx:x' fontSize={20} />
            </IconButton>
          </Box>
        }
      />
      <Handle type='target' position={Position.Left} isConnectable={true} isValidConnection={isValidConnection} />
    </Node>
  )
}

export default OutputNode
