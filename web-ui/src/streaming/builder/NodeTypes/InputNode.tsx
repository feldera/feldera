import { useEffect, useState } from 'react'
import CardHeader from '@mui/material/CardHeader'
import Avatar from '@mui/material/Avatar'
import { Box } from '@mui/material'
import IconButton from '@mui/material/IconButton'
import { Position, NodeProps, Connection, useReactFlow, getConnectedEdges } from 'reactflow'
import { useQuery } from '@tanstack/react-query'
import { Icon } from '@iconify/react'

import useNodeDelete from '../hooks/useNodeDelete'
import { Handle, Node } from '../NodeTypes'
import { connectorTypeToIcon } from 'src/types/data'
import { ConnectorDescr, ConnectorService, ConnectorType } from 'src/types/manager'

const InputNode = ({ id, data }: NodeProps) => {
  const { getNode, getEdges, deleteElements } = useReactFlow()
  const onDelete = useNodeDelete(id)

  // Fetch the connector data for the corresponding ac.connector_id
  const [connector, setConnector] = useState<ConnectorDescr | undefined>(undefined)
  const connectorQuery = useQuery(['connector', data.ac.connector_id], () =>
    ConnectorService.connectorStatus(data.ac.connector_id)
  )
  useEffect(() => {
    if (!connectorQuery.isError && !connectorQuery.isLoading) {
      setConnector(connectorQuery.data)
    }
  }, [data, connectorQuery.isError, connectorQuery.isLoading, connectorQuery.data])

  const isValidConnection = (connection: Connection) => {
    // We drop the other edge if there already is one (no more than one outgoing
    // connection from inputs).
    if (connection.source) {
      const sourceNode = getNode(connection.source)
      if (sourceNode !== undefined) {
        const edges = getConnectedEdges([sourceNode], getEdges())
        deleteElements({ nodes: [], edges })
      }
    }

    // Only allow the connection if we're going to a table.
    if (connection.target) {
      const targetNode = getNode(connection.target)

      return (
        targetNode !== undefined &&
        targetNode.type === 'sqlProgram' &&
        connection.targetHandle != null &&
        connection.targetHandle.startsWith('table-')
      )
    }

    return false
  }

  return (
    <Node>
      <CardHeader
        title={connector?.name || '<Loading>'}
        subheader={connector?.description || '<Loading>'}
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
      <Handle type='source' position={Position.Right} isConnectable={true} isValidConnection={isValidConnection} />
    </Node>
  )
}

export default InputNode
