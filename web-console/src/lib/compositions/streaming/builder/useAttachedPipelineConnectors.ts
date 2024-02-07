import { unescapeRelationName } from '$lib/functions/felderaRelation'
import { IONodeData, ProgramNodeData } from '$lib/types/connectors'
import { useReactFlow } from 'reactflow'
import invariant from 'tiny-invariant'

/**
 * Analyze existing graph nodes and edges
 * @returns A list of attached connectors according to the graph view
 */
export const useAttachedPipelineConnectors = () => {
  const { getNode, getEdges } = useReactFlow<IONodeData | ProgramNodeData>()

  return () =>
    getEdges().map(edge => {
      const source = getNode(edge.source)
      const target = getNode(edge.target)
      const connectsInput = source?.id !== 'sql'
      const connector = connectsInput ? source : target
      invariant(connector, "Couldn't extract attached connector from edge")
      invariant('ac' in connector.data, 'Wrong connector node data')
      const ac = connector.data.ac

      ac.relation_name = (handle => {
        invariant(handle, 'Node handle string should be defined')
        return unescapeRelationName(handle.replace(/^table-|^view-/, ''))
      })(connectsInput ? edge.targetHandle : edge.sourceHandle)

      return ac
    })
}
