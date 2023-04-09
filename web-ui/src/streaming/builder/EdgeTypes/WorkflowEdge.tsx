import React from 'react'
import { EdgeProps, getBezierPath } from 'reactflow'

import styled from '@emotion/styled'

const Path = styled.path`
  fill: none;
  stroke-width: 5;
  stroke: ${props => props.theme.palette.secondary.main};
`

export default function WorkflowEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd
}: EdgeProps) {
  const [edgePath, edgeCenterX, edgeCenterY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition
  })

  return (
    <>
      <Path id={id} d={edgePath} markerEnd={markerEnd} />
      <g transform={`translate(${edgeCenterX}, ${edgeCenterY})`}></g>
    </>
  )
}
