import { useCallback } from 'react'
import { NodeProps, useReactFlow } from 'reactflow'
import { useBuilderState } from '../useBuilderState'
import { sqlPlaceholderNode } from '../PipelineBuilder'
import useDebouncedSave from './useDebouncedSave'

// Logic that runs when we remove a node from the graph. Also puts back the
// sqlPlaceholder if we removed the program node, and drops all edges that were
// attached to the node.
export function useNodeDelete(id: NodeProps['id']) {
  const { addNodes, getNode, deleteElements, getEdges } = useReactFlow()
  const savePipeline = useDebouncedSave()
  const setProject = useBuilderState(state => state.setProject)

  const onClick = useCallback(() => {
    const parentNode = getNode(id)
    if (!parentNode) {
      return
    }
    const isSqlProgram = parentNode.type === 'sqlProgram'

    if (isSqlProgram) {
      setProject(undefined)
    }

    // Just hide the node(s) for now (see issue below):
    parentNode.hidden = true

    // TODO: find a better way / file a bug report
    //
    // The timer is a hack around a reactflow bug to make sure the node is
    // deleted only after the click event is finished. If we don't do this,
    // reactflow throws an error since it seems to handle some other onClick
    // handlers referencing that node. This problem seems to happen because the
    // delete button is on the node itself. It might be an alternative to just
    // have the delete button somewhere else.
    const timer = setTimeout(() => {
      deleteElements({ nodes: [parentNode] })

      // if we delete the program then all edges are affected
      if (isSqlProgram) {
        deleteElements({ edges: getEdges() })
        savePipeline()
      }

      // If we deleted the SQL program, add the placeholder back but at the same
      // position as the deleted node, make sure to reset the dimensions
      // otherwise it will look wrong
      const toAdd = isSqlProgram
        ? [{ ...sqlPlaceholderNode, width: undefined, height: undefined, position: parentNode.position }]
        : []
      if (toAdd.length > 0) {
        addNodes(toAdd)
      }
    }, 50)

    return () => clearTimeout(timer)
  }, [getNode, id, deleteElements, addNodes, getEdges, savePipeline, setProject])

  return onClick
}

export default useNodeDelete
