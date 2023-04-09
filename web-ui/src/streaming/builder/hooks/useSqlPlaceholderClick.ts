import { NodeProps, useReactFlow } from 'reactflow'
import { ProjectWithSchema } from 'src/types/program'
import { useBuilderState } from '../useBuilderState'
import useDebouncedSave from './useDebouncedSave'
import { useQueryClient } from '@tanstack/react-query'
import { ConfigDescr } from 'src/types/manager'
import { useCallback } from 'react'

// Replaces the program placeholder node with a sqlProgram node
export function useReplacePlaceholder() {
  const { setNodes, getNode } = useReactFlow()
  const replacePlaceholder = useCallback(
    (program: ProjectWithSchema) => {
      const parentNode = getNode('sql')
      if (!parentNode) {
        return
      }

      setNodes(nodes =>
        nodes.map(node => {
          // Here we are just changing the type of the clicked node from
          // placeholder to workflow
          if (node.id === parentNode.id && node.type === 'sqlPlaceholder') {
            return {
              ...node,
              width: undefined,
              height: undefined,
              type: 'sqlProgram',
              data: { label: program.name, program: program }
            }
          } else if (node.type === 'sql') {
            // Sometimes this function is called when the program node is
            // already displayed (e.g., if the query refetches). In this case,
            // we don't reset the width and height of the node (it will
            // disappear if we do).
            return {
              ...node,
              type: 'sqlProgram',
              data: { label: program.name, program: program }
            }
          } else {
            return node
          }
        })
      )
    },
    [getNode, setNodes]
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
  const configId = useBuilderState(state => state.config_id)
  const queryClient = useQueryClient()

  const onClick = useCallback(
    (_event: any, project: ProjectWithSchema) => {
      const parentNode = getNode(id)
      if (!parentNode) {
        return
      }

      setProject(project)
      if (configId) {
        queryClient.setQueryData(['configs', configId], (oldData: ConfigDescr | undefined) => {
          return oldData ? { ...oldData, project_id: project.project_id } : oldData
        })
      }
      replacePlaceholder(project)
      savePipeline()
    },
    [configId, getNode, id, queryClient, replacePlaceholder, savePipeline, setProject]
  )

  return onClick
}

export default useSqlPlaceholderClick
