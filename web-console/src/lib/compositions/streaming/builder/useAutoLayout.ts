// Computes the layout of the nodes in the graph.

import { removePrefix } from '$lib/functions/common/string'
import { ProgramDescr } from '$lib/services/manager'
import assert from 'assert'
import { useCallback, useEffect } from 'react'
import { Edge, getConnectedEdges, Instance, Node, ReactFlowState, useReactFlow, useStore } from 'reactflow'
import { escapeRelationName, quotifyRelationName } from 'src/lib/functions/felderaRelation'

// How much spacing we put after every input/output node
const VERTICAL_SPACING = 20
const HORIZONTAL_SPACING = 180

const nodeCountSelector = (state: ReactFlowState) => state.nodeInternals.size + state.edges.length
const nodesInitializedSelector = (state: ReactFlowState) =>
  Array.from(state.nodeInternals.values()).every(node => node.width && node.height)

// We want to sort input nodes so that if we connect them to a program node, the
// edges will not overlap each other.
//
// We do this by:
// - Extract and sort connectors that have an edge.
// - Merge items that don't have an edge yet with sorted connectors
//
// Also note that this approach won't work for positioning of the output node
// because they can have multiple edges.
function sortInputConnectors(nodes: Node[], edges: Edge[], tableOrder: Map<string, number>) {
  // Find all nodes that have an edge attached to them
  const nodesWithAnEdge = nodes.filter(node => {
    return edges.some(edge => edge.source === node.id)
  })

  // Find the nodes that don't have an edge and remember the original index in
  // `nodes` for them
  const nodesAndIndexWithoutAnEdge: { [index: number]: Node } = nodes
    .map((node, index) => {
      return {
        node,
        index
      }
    })
    .filter(element => {
      return edges.every(edge => edge.source !== element.node.id)
    })
    .reduce((obj, item) => ({ ...obj, [item.index]: item.node }), {})

  nodesWithAnEdge.sort((a, b) => {
    const aEdges = getConnectedEdges([a], edges)
    const bEdges = getConnectedEdges([b], edges)
    assert(aEdges.length === 1)
    assert(bEdges.length === 1)
    assert(aEdges[0].targetHandle && bEdges[0].targetHandle)

    const aTable = removePrefix(aEdges[0].targetHandle, 'table-')
    const bTable = removePrefix(bEdges[0].targetHandle, 'table-')
    assert(aTable && bTable)

    const aIndex = tableOrder.get(aTable)
    const bIndex = tableOrder.get(bTable)
    assert(aIndex !== undefined && bIndex !== undefined)

    return aIndex - bIndex
  })

  // Merge the two arrays
  const result = []
  let sortedIndex = 0
  for (let i = 0; i < nodes.length; i++) {
    if (!nodesAndIndexWithoutAnEdge[i]) {
      result.push(nodesWithAnEdge[sortedIndex])
      sortedIndex++
    } else {
      result.push(nodes[i])
    }
  }

  return result
}

// Positions the nodes in a fixed layout. The layout is:
//
// - Input nodes are on the left
// - Output nodes are on the right
// - Program node is in the middle
// - The Input and Output nodes are evenly spaced vertically.
// - The program node is centered vertically wrt. to the input and output nodes.
// - Input nodes are reordered based on which table they are connected to.
//
// The input and output nodes are evenly spaced horizontally from the program
// node. The output nodes are left aligned, the input nodes are right-aligned.
function layoutNodesFixed(
  getNodes: Instance.GetNodes<any>,
  setNodes: Instance.SetNodes<any>,
  getEdges: Instance.GetEdges<any>
) {
  const nodes: Node[] = getNodes()
  const programNode = nodes.filter(node => node.type === 'sqlPlaceholder' || node.type === 'sqlProgram')
  assert(programNode.length === 1)

  const rawInputNodes = nodes.filter(node => node.type === 'inputNode')
  const inputOrder = new Map<string, number>()
  if (programNode[0].data?.program?.schema) {
    const schema = programNode[0].data.program.schema as NonNullable<ProgramDescr['schema']>
    schema.inputs.forEach((relation, index: number) => {
      inputOrder.set(escapeRelationName(quotifyRelationName(relation)), index)
    })
  }
  const inputNodes = sortInputConnectors(rawInputNodes, getEdges(), inputOrder).concat(
    nodes.filter(node => node.id === 'inputPlaceholder')
  )

  const outputNodes = nodes
    .filter(node => node.type === 'outputNode' || node.id === 'outputPlaceholder')
    .sort((a, b) => a.position.y - b.position.y)

  // Vertical adjustment
  //
  // Calculate how much vertical space input/output nodes consume (not
  // including the placeholder nodes at the end).
  //
  // We want to:
  // - Keep the input and output nodes evenly spaced.
  // - Keep the program window centered vertically wrt. to the input and output nodes
  const totalInputHeight = inputNodes
    .filter(node => node.type === 'inputNode')
    .map(node => {
      return (node.height || 0) + VERTICAL_SPACING
    })
    .reduce((a, b) => a + b, 0)
  const totalOutputHeight = outputNodes
    .filter(node => node.type === 'outputNode')
    .map(node => {
      return (node.height || 0) + VERTICAL_SPACING
    })
    .reduce((a, b) => a + b, 0)

  // Horizontal adjustment
  //
  // We right align the input column based on the widest input node. We make
  // sure the space between input/program and program/output is the same.
  const maxInputWidth = inputNodes.map(node => node.width || 0).reduce((a, b) => Math.max(a, b), 0)
  const programWidth = programNode.map(node => node.width || 0).reduce((a, b) => Math.max(a, b), 0)
  const programHeight = programNode.map(node => node.height || 0).reduce((a, b) => Math.max(a, b), 0)

  // define initial x-axis position for input, output and program based in maxInputWidth, programWidth and maxOutputWidth
  const HORIZONTAL_START_OFFSET = 200
  const inputXPos = HORIZONTAL_START_OFFSET - (maxInputWidth + programWidth / 2 + HORIZONTAL_SPACING)
  const programXPos = HORIZONTAL_START_OFFSET - programWidth / 2
  const outputXPos = HORIZONTAL_START_OFFSET + programWidth / 2 + HORIZONTAL_SPACING
  const maxHeight = Math.max(totalInputHeight, totalOutputHeight, programHeight)

  // Adjust the input node y position: Center with respect to the output nodes
  let curInputYPos = 0
  const adjustedInputNodes = inputNodes.map((node, idx) => {
    // space out input nodes evenly vertical
    if (idx == 0) {
      curInputYPos = maxHeight / 2 - totalInputHeight / 2
    } else {
      curInputYPos += (inputNodes[idx - 1].height || 0) + VERTICAL_SPACING
    }

    return { ...node, position: { x: inputXPos + (maxInputWidth - (node.width || 0)), y: curInputYPos } }
  })

  // Adjust the output node y positions: Center with respect to the input
  // nodes
  let curOutputYPos = 0
  const adjustedOutputNodes = outputNodes.map((node, idx) => {
    // space out input nodes evenly vertical
    if (idx == 0) {
      curOutputYPos = maxHeight / 2 - totalOutputHeight / 2
    } else {
      curOutputYPos += (outputNodes[idx - 1].height || 0) + VERTICAL_SPACING
    }

    return { ...node, position: { x: outputXPos, y: curOutputYPos } }
  })

  // Reset the nodes in the graph, we already have adjusted input and output
  // nodes, we still need calculate the position of the sql placeholder or
  // program node (we do it inline)
  setNodes(nodes => {
    const newNodes = nodes
      .filter(node => node.type === 'sqlPlaceholder' || node.type === 'sqlProgram')
      .map(node => {
        // Layout the placeholder nodes
        if (node.type === 'sqlPlaceholder' || node.type === 'sqlProgram') {
          return {
            ...node,
            position: { x: programXPos, y: maxHeight / 2 - (node.height || 0) / 2 }
          }
        } else {
          return node
        }
      })
      .concat(adjustedInputNodes)
      .concat(adjustedOutputNodes)

    return newNodes
  })
}

// Does positioning of the nodes in the graph.
export function useRedoLayout() {
  const nodeCount = useStore(nodeCountSelector)
  const nodesInitialized = useStore(nodesInitializedSelector)
  const { getNodes, setNodes, getEdges } = useReactFlow()

  return useCallback(() => {
    if (!nodeCount || !nodesInitialized) {
      return
    }
    layoutNodesFixed(getNodes, setNodes, getEdges)
  }, [nodeCount, nodesInitialized, getNodes, setNodes, getEdges])
}

// Does positioning of the nodes in the graph.
function useAutoLayout() {
  const redoLayout = useRedoLayout()

  useEffect(() => {
    redoLayout()
  }, [redoLayout])
}

export default useAutoLayout
