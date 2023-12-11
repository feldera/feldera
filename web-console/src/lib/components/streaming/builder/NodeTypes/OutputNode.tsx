// OutputNodes are displayed on the right and connected with views of the
// program.

import { useDeleteNode } from '$lib/compositions/streaming/builder/useDeleteNode'
import { useDeleteDialog } from '$lib/compositions/useDialog'
import { connectorDescrToType, connectorTypeToIcon } from '$lib/functions/connectors'
import { ConnectorDescr } from '$lib/services/manager'
import { Connection, NodeProps, Position, useReactFlow } from 'reactflow'
import IconX from '~icons/bx/x'

import { Avatar, Box, Link, useTheme } from '@mui/material'
import CardHeader from '@mui/material/CardHeader'
import IconButton from '@mui/material/IconButton'

import { Handle, Node } from '../NodeTypes'

const OutputNode = ({ id, data }: NodeProps<{ connector: ConnectorDescr }>) => {
  const theme = useTheme()
  const { getNode } = useReactFlow()
  const onDelete = useDeleteNode(() => {})(id)

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

  const { showDeleteDialog } = useDeleteDialog()

  return (
    <Node>
      <Link href={`#edit/connector/${data.connector.connector_id}`}>
        <CardHeader
          title={data.connector.name}
          subheader={data.connector.description}
          sx={{ py: 5, pl: 12, alignItems: 'flex-start' }}
          titleTypographyProps={{ variant: 'h5' }}
          subheaderTypographyProps={{ variant: 'body1', sx: { color: 'text.disabled' } }}
          avatar={
            <Avatar variant='rounded' sx={{ mt: 1.5, width: 42, height: 42 }}>
              {(Icon => {
                return (
                  <Icon
                    style={{
                      width: '90%',
                      height: '90%',
                      fill: theme.palette.text.primary,
                      color: theme.palette.text.primary
                    }}
                  ></Icon>
                )
              })(connectorTypeToIcon(connectorDescrToType(data.connector)))}
            </Avatar>
          }
        />
      </Link>
      <Box
        sx={{ display: 'flex', alignItems: 'center', position: 'absolute', top: 4, right: 4 }}
        className='nodrag nopan'
      >
        <IconButton
          size='small'
          aria-label='close'
          sx={{ color: 'text.secondary' }}
          onClick={showDeleteDialog(
            'Remove',
            `${data.connector.name || 'unnamed'} output`,
            onDelete,
            'You can add it back later.'
          )}
        >
          <IconX fontSize={20} />
        </IconButton>
      </Box>
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
