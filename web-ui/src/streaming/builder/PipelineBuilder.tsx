// The pipeline builder lets you configure a pipeline by adding connectors to
// tables and views of a program.

import React, { useCallback, useRef } from 'react'
import ReactFlow, {
  Background,
  Connection,
  Edge,
  EdgeChange,
  MarkerType,
  Node,
  ProOptions,
  updateEdge,
  useReactFlow
} from 'reactflow'
import useAutoLayout, { useRedoLayout } from './hooks/useAutoLayout'
import nodeTypes from './NodeTypes'
import edgeTypes from './EdgeTypes'
import AddSourceDrawer from 'src/connectors/drawer/AddSourceDrawer'
import useDebouncedSave from './hooks/useDebouncedSave'

import 'reactflow/dist/style.css'

const proOptions: ProOptions = { account: 'paid-pro', hideAttribution: true }

export const sqlPlaceholderNode: Node = {
  id: 'sql',
  data: { label: 'Select a SQL program', icon: 'ant-design:console-sql-outlined' },
  position: { x: 0, y: 150 },
  type: 'sqlPlaceholder',
  deletable: false
}

// Initial layout of the window: We add the three placeholder nodes.
//
// The sqlPlaceholderNode gets removed/replaced with a SQL program by the user.
// The inputPlaceholder and outputPlaceholder always remain visible.
const defaultNodes: Node[] = [
  {
    id: 'inputPlaceholder',
    data: { label: 'Add a new Input', icon: 'bx:arrow-from-left' },
    position: { x: -500, y: 150 },
    type: 'ioPlaceholder',
    deletable: false
  },
  sqlPlaceholderNode,
  {
    id: 'outputPlaceholder',
    data: { label: 'Add a new Output', icon: 'bx:arrow-to-right' },
    position: { x: 500, y: 150 },
    type: 'ioPlaceholder',
    deletable: false
  }
]

// initial setup: no edges
const defaultEdges: Edge[] = []

const fitViewOptions = {
  padding: 0.95
}

function PipelineGraphWindow() {
  const { setEdges, deleteElements } = useReactFlow()
  const savePipeline = useDebouncedSave()
  const redoLayout = useRedoLayout()
  useAutoLayout()
  const edgeUpdateSuccessful = useRef(true)

  const onEdgeUpdateStart = useCallback(() => {
    edgeUpdateSuccessful.current = false
  }, [])

  // Callback when an existing edge changes target or source
  const onEdgeUpdate = useCallback(
    (oldEdge: Edge, newConnection: Connection) => {
      edgeUpdateSuccessful.current = true
      setEdges(els => updateEdge(oldEdge, newConnection, els))
      savePipeline()
      redoLayout()
    },
    [setEdges, savePipeline, redoLayout]
  )

  const onEdgeUpdateEnd = useCallback(
    (_: any, edge: Edge) => {
      if (!edgeUpdateSuccessful.current) {
        deleteElements({ nodes: [], edges: [edge] })
      }
      edgeUpdateSuccessful.current = true
    },
    [deleteElements]
  )

  // Callback when a new edge is created
  const onConnect = useCallback(() => {
    savePipeline()
  }, [savePipeline])

  // Callback when an edge is removed
  const onEdgeChange = useCallback(
    (edgeChanges: EdgeChange[]) => {
      edgeChanges.forEach(edgeChange => {
        if (edgeChange.type == 'remove') {
          savePipeline()
        }
      })
    },
    [savePipeline]
  )

  // This should be inside WorkflowEdge but I couldn't figure out how to put
  // this directly on markerEnd in WorkflowEdge
  const defaultEdgeOptions = {
    type: 'inspectableEdge',
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 8,
      height: 8
    },
    animated: true
  }

  return (
    <>
      <ReactFlow
        defaultNodes={defaultNodes}
        defaultEdges={defaultEdges}
        defaultEdgeOptions={defaultEdgeOptions}
        edgesFocusable={true}
        onEdgeUpdateStart={onEdgeUpdateStart}
        onEdgeUpdate={onEdgeUpdate}
        onEdgeUpdateEnd={onEdgeUpdateEnd}
        onEdgesChange={onEdgeChange}
        onConnect={onConnect}
        proOptions={proOptions}
        fitView
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitViewOptions={fitViewOptions}
        minZoom={0.5}
        maxZoom={1.5}
        nodesDraggable={false}
        nodesConnectable={true}
        zoomOnDoubleClick={false}
        snapToGrid={true}
        onlyRenderVisibleElements={true}
      >
        <Background />
      </ReactFlow>
      <AddSourceDrawer />
    </>
  )
}

function PipelineGraph() {
  return <PipelineGraphWindow />
}

export default PipelineGraph
