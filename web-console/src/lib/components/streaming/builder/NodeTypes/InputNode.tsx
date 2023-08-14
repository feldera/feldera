// InputNodes are on the left and connect to tables of the program.

import useNodeDelete from '$lib/compositions/streaming/builder/useNodeDelete'
import { connectorDescrToType, connectorTypeToIcon } from '$lib/functions/connectors'
import { Connection, getConnectedEdges, NodeProps, Position, useReactFlow } from 'reactflow'

import { Icon } from '@iconify/react'
import { Box } from '@mui/material'
import Avatar from '@mui/material/Avatar'
import CardHeader from '@mui/material/CardHeader'
import IconButton from '@mui/material/IconButton'

import { Handle, Node } from '../NodeTypes'

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
            <Icon icon={connectorTypeToIcon(connectorDescrToType(data.connector))} />
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
      {/* The .inputHandle is referenced by webui-tester */}
      <Handle
        className='inputHandle'
        type='source'
        position={Position.Right}
        isConnectable={true}
        isValidConnection={isValidConnection}
      />
    </Node>
  )
}

export default InputNode
