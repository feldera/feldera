// OutputNodes are displayed on the right and connected with views of the
// program.

import useNodeDelete from '$lib/compositions/streaming/builder/useNodeDelete'
import { connectorDescrToType, connectorTypeToIcon } from '$lib/functions/connectors'
import { Connection, NodeProps, Position, useReactFlow } from 'reactflow'

import { Icon } from '@iconify/react'
import { Box } from '@mui/material'
import Avatar from '@mui/material/Avatar'
import CardHeader from '@mui/material/CardHeader'
import IconButton from '@mui/material/IconButton'

import { Handle, Node } from '../NodeTypes'

const OutputNode = ({ id, data }: NodeProps) => {
  const { getNode } = useReactFlow()
  const onDelete = useNodeDelete(id)

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
      {/* The .outputHandle is referenced by webui-tester */}
      <Handle
        className='outputHandle'
        type='target'
        position={Position.Left}
        isConnectable={true}
        isValidConnection={isValidConnection}
      />
    </Node>
  )
}

export default OutputNode
