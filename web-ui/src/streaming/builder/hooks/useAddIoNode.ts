// Logic to add either a input or output node to the graph.

import { useCallback } from 'react'
import { useReactFlow, getConnectedEdges } from 'reactflow'
import { AttachedConnector, Direction } from 'src/types/manager'
import { ConnectorDescr } from 'src/types/manager/models/ConnectorDescr'
import { Schema } from 'src/types/program'
import { randomString } from 'src/utils'
import { useRedoLayout } from './useAutoLayout'

const HEIGHT_OFFSET = 120

// Checks if the connector can connect to a give schema
export function connectorConnects(ac: AttachedConnector, schema: Schema): boolean {
  console.log('connectorConnects', ac, schema)
  if (ac.direction === Direction.INPUT) {
    return schema.inputs.some(table => table.name === ac.config)
  } else {
    return schema.outputs.some(view => view.name === ac.config)
  }
}

export function useAddConnector() {
  const { setNodes, getNodes, getNode, addNodes, addEdges } = useReactFlow()
  const redoLayout = useRedoLayout()

  const addNewConnector = useCallback(
    (connector: ConnectorDescr, ac: AttachedConnector) => {
      // Input or Output?
      const newNodeType = ac.direction === Direction.INPUT ? 'inputNode' : 'outputNode'
      const placeholderId = ac.direction === Direction.INPUT ? 'inputPlaceholder' : 'outputPlaceholder'
      const placeholder = getNode(placeholderId)
      if (!placeholder) {
        return
      }

      // If this node already exists, don't add it again
      const existingNode = getNodes().find(node => node.id === ac.uuid)
      if (existingNode) {
        return
      } else {
        // Move the placeholder node down a bit; useAutoLayout will eventually
        // also place it at the right spot, but it looks better when it happens
        // here immediately.
        setNodes(nodes =>
          nodes.map(node => {
            if (node.id === placeholderId) {
              return {
                ...node,
                position: { x: placeholder.position.x, y: placeholder.position.y + HEIGHT_OFFSET }
              }
            }

            return node
          })
        )

        // Add the new nodes
        addNodes({
          position: { x: placeholder.position.x, y: placeholder.position.y },
          id: ac.uuid,
          type: newNodeType,
          deletable: true,
          data: { connector, ac }
        })
        redoLayout()
      }

      // Now that we have the node, we need to add a connector if we have one
      const sqlNode = getNode('sql')
      const ourNode = getNode(ac.uuid)
      const tableOrView = ac.config
      const sqlPrefix = ac.direction === Direction.INPUT ? 'table-' : 'view-'
      const connectorHandle = sqlPrefix + tableOrView
      const hasAnEdge = ac.config != ''

      if (hasAnEdge && sqlNode && ourNode) {
        const existingEdge = getConnectedEdges([sqlNode, ourNode], []).find(
          edge => edge.targetHandle === connectorHandle || edge.sourceHandle === connectorHandle
        )

        if (!existingEdge) {
          const sourceId = ac.direction === Direction.INPUT ? ac.uuid : 'sql'
          const targetId = ac.direction === Direction.INPUT ? 'sql' : ac.uuid
          const sourceHandle = ac.direction === Direction.INPUT ? null : connectorHandle
          const targetHandle = ac.direction === Direction.INPUT ? connectorHandle : null

          addEdges({
            id: randomString(),
            source: sourceId,
            target: targetId,
            sourceHandle: sourceHandle,
            targetHandle: targetHandle
          })
        }
      }
    },
    [getNode, getNodes, setNodes, addNodes, addEdges]
  )

  return addNewConnector
}
