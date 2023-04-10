import { Handle as RawHandle, NodeTypes } from 'reactflow'
import styled from '@emotion/styled'

import IOPlaceholderNode from './IOPlaceholderNode'
import SqlPlaceholderNode from './SqlPlaceholderNode'

import InputNode from './InputNode'
import OutputNode from './OutputNode'
import Card from '@mui/material/Card'
import SqlNode from './SqlNode'
import Box from '@mui/material/Box'

export const Handle = styled(RawHandle)`
  background-color: ${({ theme }) => theme.palette.primary.main};
  border: 8px solid ${({ theme }) => theme.palette.background.default};
  width: 35px;
  height: 35px;
  border-radius: 25px;
`

export const Node = styled(Card)`
  box-shadow: rgba(0, 0, 0, 0.08) 0px 4px 12px;
  text-align: center;
  font-weight: bold;
  color: #0984e3;
  cursor: default;
`

export const InnerNode = styled(Box)`
  cursor: default;
`

export const PlaceholderNode = styled(Card)`
  width: 310px;
  border: 1px dashed #bbb;
  color: #bbb;
  box-shadow: none;
  cursor: pointer;
`

const nodeTypes: NodeTypes = {
  ioPlaceholder: IOPlaceholderNode,
  inputNode: InputNode,
  outputNode: OutputNode,
  sqlPlaceholder: SqlPlaceholderNode,
  sqlProgram: SqlNode
}

export default nodeTypes
