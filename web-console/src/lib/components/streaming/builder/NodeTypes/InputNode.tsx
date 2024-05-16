// InputNodes are on the left and connect to tables of the program.

import { Handle, Node } from '$lib/components/streaming/builder/NodeTypes'
import { useDeleteNode } from '$lib/compositions/streaming/builder/useDeleteNode'
import { useDeleteDialog } from '$lib/compositions/useDialog'
import { connectorDescrToType, connectorTypeToIcon } from '$lib/functions/connectors'
import { ConnectorDescr } from '$lib/services/manager'
import { Connection, getConnectedEdges, NodeProps, Position, useReactFlow } from 'reactflow'

import { Box, Link, useTheme } from '@mui/material'
import Avatar from '@mui/material/Avatar'
import CardHeader from '@mui/material/CardHeader'
import IconButton from '@mui/material/IconButton'

const InputNode = ({ id, data }: NodeProps<{ connector: ConnectorDescr }>) => {
  const theme = useTheme()
  const { getNode, getEdges, deleteElements } = useReactFlow()
  const onDelete = useDeleteNode(() => {})(id)

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
        connection.targetHandle !== null &&
        connection.targetHandle.startsWith('table-')
      )
    } else {
      return false
    }
  }

  const { showDeleteDialog } = useDeleteDialog()
  return (
    <Node>
      <Link href={`#edit/connector/${data.connector.name}`}>
        <CardHeader
          title={data.connector.name}
          subheader={data.connector.description}
          sx={{ py: 5, pr: 12, alignItems: 'flex-start' }}
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
              })(connectorTypeToIcon(connectorDescrToType(data.connector.config)))}
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
          onClick={showDeleteDialog('Remove', `${data.connector.name} input`, onDelete, 'You can add it back later.')}
        >
          <i className={`bx bx-x`} style={{ fontSize: 20 }} />
        </IconButton>
      </Box>
      <Handle
        type='source'
        position={Position.Right}
        isConnectable={true}
        isValidConnection={isValidConnection}
        data-testid={'box-handle-input-' + data.connector.name}
      />
    </Node>
  )
}

export default InputNode
