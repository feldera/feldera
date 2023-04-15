// What happens when we select a program in the sqlPlaceholder node.

import { NodeProps, useReactFlow, useUpdateNodeInternals } from 'reactflow'
import { ProjectWithSchema } from 'src/types/program'
import { useBuilderState } from '../useBuilderState'
import useDebouncedSave from './useDebouncedSave'
import { useQueryClient } from '@tanstack/react-query'
import { ConfigDescr } from 'src/types/manager'
import { useCallback } from 'react'

// Replaces the program placeholder node with a sqlProgram node
export function useReplacePlaceholder() {
  const { getNode, deleteElements, addNodes } = useReactFlow()
  const updateNodeInternals = useUpdateNodeInternals();

  const replacePlaceholder = useCallback(
    (program: ProjectWithSchema) => {
      const parentNode = getNode('sql')
      if (!parentNode) {
        return
      }
      deleteElements({nodes: [parentNode]})
      addNodes({
        ...parentNode,
        type: 'sqlProgram',
        data: { label: program.name, program: program }
      })
      updateNodeInternals(parentNode.id)
    },
    [getNode, addNodes, deleteElements, updateNodeInternals]
  )

  return replacePlaceholder
}

// this hook implements the logic for clicking the sql placeholder node: we
// convert it to a sqlProgram node.
export function useSqlPlaceholderClick(id: NodeProps['id']) {
  const { getNode } = useReactFlow()
  const savePipeline = useDebouncedSave()
  const replacePlaceholder = useReplacePlaceholder()
  const setProject = useBuilderState(state => state.setProject)
  const queryClient = useQueryClient()

  const onClick = useCallback(
    (_event: any, project: ProjectWithSchema) => {
      const parentNode = getNode(id)
      if (!parentNode) {
        return
      }

      setProject(project)
      const configId = null
      if (configId) {
        // TODO: figure out if it's better to optimistically update query here?
        queryClient.setQueryData(['configStatus', {config_id: configId}], (oldData: ConfigDescr | undefined) => {
          return oldData ? { ...oldData, project_id: project.project_id } : oldData
        })
      }
      replacePlaceholder(project)
      savePipeline()
    },
    [getNode, id, queryClient, replacePlaceholder, savePipeline, setProject]
  )

  return onClick
}

export default useSqlPlaceholderClick
