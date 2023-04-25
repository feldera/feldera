// SqlNode is a (compiled) program with tables and view as handles to connect
// to.

import { memo } from 'react'
import { Connection, getConnectedEdges, NodeProps, Position, useReactFlow } from 'reactflow'
import { Box, CardContent, CardHeader, Stack } from '@mui/material'
import { Handle, Node } from '../NodeTypes'
import IconButton from '@mui/material/IconButton'
import useNodeDelete from '../hooks/useNodeDelete'

import Chip from '@mui/material/Chip'
import Avatar from '@mui/material/Avatar'
import { Icon } from '@iconify/react'
import { zip } from 'src/utils'

function SqlTableNode(props: { name: string }) {
  const { getNode, getEdges } = useReactFlow()

  // Only allow the connection if it's coming from an input node
  const isValidConnection = (connection: Connection) => {
    if (connection.source) {
      const sourceNode = getNode(connection.source)
      if (!sourceNode) {
        return false
      }
      const sourceAlreadyHasEdge = getConnectedEdges([sourceNode], getEdges()).length > 0
      return sourceNode !== undefined && !sourceAlreadyHasEdge && sourceNode.type === 'inputNode'
    } else {
      return false
    }
  }

  return (
    <Box sx={{ ml: -6, mt: 3 }} style={{ position: 'relative' }}>
      <Chip
        sx={{ ml: 9 }}
        label={props.name}
        color='secondary'
        avatar={
          <Avatar>
            <Icon fontSize={16} icon='mdi:database-import' />
          </Avatar>
        }
      />
      {/* The table- prefix is important for the isValidConnection functions */}
      <Handle
        id={'table-' + props.name}
        type='target'
        position={Position.Left}
        isConnectable={true}
        isValidConnection={isValidConnection}
      />
    </Box>
  )
}

function SqlViewNode(props: { name: string }) {
  const { getNode } = useReactFlow()

  // Only allow the connection if we're going from a view to an output node
  const isValidConnection = (connection: Connection) => {
    if (connection.target) {
      const targetNode = getNode(connection.target)
      return targetNode !== undefined && targetNode.type === 'outputNode'
    } else {
      return false
    }
  }

  return (
    <Box sx={{ mr: -6, mt: 3, textAlign: 'right' }} style={{ position: 'relative' }}>
      <Chip
        sx={{ mr: 9 }}
        label={props.name}
        color='secondary'
        avatar={
          <Avatar>
            <Icon fontSize={16} icon='mdi:database-export' />
          </Avatar>
        }
      />
      {/* The view- prefix is important for the isValidConnection functions */}
      <Handle
        id={'view-' + props.name}
        type='source'
        position={Position.Right}
        isConnectable={true}
        isValidConnection={isValidConnection}
      />
    </Box>
  )
}

function SqlNode({ id, data }: NodeProps) {
  const onDelete = useNodeDelete(id)

  return (
    <Node>
      <CardHeader
        title={data.label}
        sx={{ py: 5, alignItems: 'flex-start' }}
        titleTypographyProps={{ variant: 'h4' }}
        avatar={<Icon icon='ant-design:console-sql-outlined' fontSize='2rem' />}
        action={
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <IconButton size='small' aria-label='close' sx={{ color: 'text.secondary' }} onClick={() => onDelete()}>
              <Icon icon='bx:x' fontSize={20} />
            </IconButton>
          </Box>
        }
      />

      <CardContent sx={{ textAlign: 'center', '& svg': { mb: 2 } }}>
        {zip(data.program.schema.inputs, data.program.schema.outputs).map((e, idx) => {
          const input = e[0]
          const output = e[1]

          return (
            <Stack direction='row' spacing={2} key={idx} sx={{ width: '100%' }}>
              <Box sx={{ textAlign: 'left' }}>{input && <SqlTableNode name={input.name} />}</Box>
              <Box sx={{ textAlign: 'right', width: '100%' }}> {output && <SqlViewNode name={output.name} />}</Box>
            </Stack>
          )
        })}
      </CardContent>
    </Node>
  )
}

export default memo(SqlNode)
