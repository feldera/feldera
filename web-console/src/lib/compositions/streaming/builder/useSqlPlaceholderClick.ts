// What happens when we select a program in the sqlPlaceholder node.

import { Pipeline, ProgramDescr } from '$lib/services/manager'
import { useCallback } from 'react'
import { NodeProps, useReactFlow } from 'reactflow'

import { useQueryClient } from '@tanstack/react-query'

import { useBuilderState } from './useBuilderState'
import useDebouncedSave from './useDebouncedSave'

// Replaces the program placeholder node with a sqlProgram node
export function useReplacePlaceholder() {
  const { getNode, setNodes } = useReactFlow()

  const replacePlaceholder = useCallback(
    (program: ProgramDescr) => {
      console.log(program)
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
  const queryClient = useQueryClient()

  const onClick = useCallback(
    (_event: any, project: ProgramDescr) => {
      const parentNode = getNode(id)
      if (!parentNode) {
        return
      }

      setProject(project)
      const pipelineId = null
      if (pipelineId) {
        // TODO: figure out if it's better to optimistically update query here?
        queryClient.setQueryData(['pipelineStatus', { pipeline_id: pipelineId }], (oldData: Pipeline | undefined) => {
          return oldData
            ? { ...oldData, descriptor: { ...oldData.descriptor, program_id: project.program_id } }
            : oldData
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
