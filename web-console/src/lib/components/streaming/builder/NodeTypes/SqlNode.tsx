// SqlNode is a (compiled) program with tables and view as handles to connect
// to.

import { Handle, Node } from '$lib/components/streaming/builder/NodeTypes'
import { useDeleteNode, useDeleteNodeProgram } from '$lib/compositions/streaming/builder/useDeleteNode'
import { useDeleteDialog } from '$lib/compositions/useDialog'
import { zipDefault } from '$lib/functions/common/tuple'
import { escapeRelationName, quotifyRelationName } from '$lib/functions/felderaRelation'
import { ProgramDescr, Relation } from '$lib/services/manager'
import { Connection, getConnectedEdges, NodeProps, Position, useReactFlow } from 'reactflow'
import { TextIcon } from 'src/lib/components/common/TextIcon'
import IconX from '~icons/bx/x'
import IconDatabaseExport from '~icons/mdi/database-export'
import IconDatabaseImport from '~icons/mdi/database-import'

import { Box, CardContent, CardHeader, Stack } from '@mui/material'
import Avatar from '@mui/material/Avatar'
import Chip from '@mui/material/Chip'
import IconButton from '@mui/material/IconButton'

function SqlTableNode(props: { relation: Relation }) {
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
        sx={{ ml: 9, '.MuiChip-label': { textTransform: 'none' } }}
        label={quotifyRelationName(props.relation)}
        color='secondary'
        avatar={
          <Avatar>
            <IconDatabaseImport fontSize={16} />
          </Avatar>
        }
      />
      {/* The table- prefix is important for the isValidConnection logic */}
      <Handle
        id={'table-' + escapeRelationName(quotifyRelationName(props.relation))}
        type='target'
        position={Position.Left}
        isConnectable={true}
        isValidConnection={isValidConnection}
        data-testid={'box-handle-table-' + quotifyRelationName(props.relation)}
      />
    </Box>
  )
}

function SqlViewNode(props: { relation: Relation }) {
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
        sx={{ mr: 9, '.MuiChip-label': { textTransform: 'none' } }}
        label={quotifyRelationName(props.relation)}
        color='secondary'
        avatar={
          <Avatar>
            <IconDatabaseExport fontSize={16} />
          </Avatar>
        }
      />
      {/* The view- prefix is important for the isValidConnection functions */}
      <Handle
        id={'view-' + escapeRelationName(quotifyRelationName(props.relation))}
        type='source'
        position={Position.Right}
        isConnectable={true}
        isValidConnection={isValidConnection}
        data-testid={'box-handle-view-' + quotifyRelationName(props.relation)}
      />
    </Box>
  )
}

export function SqlNode({ id, data }: NodeProps<{ label: string; program: ProgramDescr }>) {
  const onDelete = useDeleteNode(useDeleteNodeProgram())(id)

  const inputs = data.program.schema?.inputs || []
  const outputs = data.program.schema?.outputs || []

  const { showDeleteDialog } = useDeleteDialog()

  return (
    <Node>
      <CardHeader
        title={data.label}
        sx={{ py: 5, alignItems: 'flex-start' }}
        titleTypographyProps={{ variant: 'h4' }}
        avatar={<TextIcon text='SQL' fontSize={14} size={32} />}
        action={
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <IconButton
              size='small'
              aria-label='close'
              sx={{ color: 'text.secondary' }}
              onClick={showDeleteDialog(
                'Remove',
                `${data.program.name || 'unnamed'} program`,
                onDelete,
                'You can add it back later.'
              )}
              data-testid='button-remove-program'
            >
              <IconX fontSize={20} />
            </IconButton>
          </Box>
        }
      />

      <CardContent sx={{ textAlign: 'center', '& svg': { mb: 2 } }}>
        {zipDefault<Relation | undefined, Relation | undefined>(undefined, undefined)(inputs, outputs).map(
          ([input, output], idx) => {
            return (
              <Stack direction='row' spacing={2} key={idx} sx={{ width: '100%' }}>
                <Box sx={{ textAlign: 'left' }}>{input && <SqlTableNode relation={input} />}</Box>
                <Box sx={{ textAlign: 'right', width: '100%' }}> {output && <SqlViewNode relation={output} />}</Box>
              </Stack>
            )
          }
        )}
      </CardContent>
    </Node>
  )
}
