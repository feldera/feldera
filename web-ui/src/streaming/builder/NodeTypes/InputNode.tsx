// InputNodes are on the left and connect to tables of the program.

import CardHeader from '@mui/material/CardHeader'
import Avatar from '@mui/material/Avatar'
import { Box } from '@mui/material'
import IconButton from '@mui/material/IconButton'
import { Position, NodeProps, Connection, useReactFlow, getConnectedEdges } from 'reactflow'
import { Icon } from '@iconify/react'

import useNodeDelete from '../hooks/useNodeDelete'
import { Handle, Node } from '../NodeTypes'
import { connectorTypeToIcon } from 'src/types/connectors'

const InputNode = ({ id, data }: NodeProps) => {
  const { getNode, getEdges, deleteElements } = useReactFlow()
  const onDelete = useNodeDelete(id)

  const isValidConnection = (connection: Connection) => {
    // We drop the other edge if there already is one (no more than one outgoing
    // connection from each input).
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
    } else {
      return false
    }
  }

  return (
    <Node>
      <CardHeader
        title={data.connector.name}
        subheader={data.connector.description}
        sx={{ py: 5, alignItems: 'flex-start' }}
        titleTypographyProps={{ variant: 'h5' }}
        subheaderTypographyProps={{ variant: 'body1', sx: { color: 'text.disabled' } }}
        avatar={
          <Avatar sx={{ mt: 1.5, width: 42, height: 42 }}>
            <Icon icon={connectorTypeToIcon(data.connector.typ)} />
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
