// What happens when we select a program in the sqlPlaceholder node.

import { useAttachedPipelineConnectors } from '$lib/compositions/streaming/builder/useAttachedPipelineConnectors'
import { useBuilderState } from '$lib/compositions/streaming/builder/useBuilderState'
import { useUpdatePipeline } from '$lib/compositions/streaming/builder/useUpdatePipeline'
import { ProgramDescr } from '$lib/services/manager'
import { useCallback } from 'react'
import { NodeProps, useReactFlow } from 'reactflow'

// import { useQueryClient } from '@tanstack/react-query'

// Replaces the program placeholder node with a sqlProgram node
export function useReplacePlaceholder() {
  const { getNode, setNodes } = useReactFlow()

  const replacePlaceholder = useCallback(
    (program: ProgramDescr) => {
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
  const replacePlaceholder = useReplacePlaceholder()
  const attachedPipelineConnectors = useAttachedPipelineConnectors()
  // const queryClient = useQueryClient()

  const updatePipeline = useUpdatePipeline(
    useBuilderState(s => s.pipelineName),
    useBuilderState(s => s.setSaveState),
    useBuilderState(s => s.setFormError)
  )

  const onClick = useCallback(
    (_event: any, project: ProgramDescr) => {
      const parentNode = getNode(id)
      if (!parentNode) {
        return
      }
      replacePlaceholder(project)
      // savePipeline()
      updatePipeline(p => ({ ...p, program_name: project.name, connectors: attachedPipelineConnectors() }))
    },
    [getNode, id, replacePlaceholder, updatePipeline, attachedPipelineConnectors]
  )

  return onClick
}

export default useSqlPlaceholderClick
