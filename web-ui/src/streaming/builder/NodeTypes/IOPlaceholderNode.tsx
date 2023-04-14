// The placeholder for inputs, opens the drawer to add input connectors on a
// click.

import React, { memo } from 'react'
import { NodeProps } from 'reactflow'
import useIOPlaceholderClick from '../hooks/useIOPlaceholderClick'
import { Icon } from '@iconify/react'
import { CardContent, Typography } from '@mui/material'
import { PlaceholderNode } from '.'

const IOPlaceholderNode = ({ id, data }: NodeProps) => {
  // see the hook implementation for details of the click handler
  // calling onClick turns this node and the connecting edge into a workflow node
  const onClick = useIOPlaceholderClick(id)

  return (
    <PlaceholderNode onClick={onClick}>
      <CardContent sx={{ textAlign: 'center' }}>
        <Icon icon={data.icon} fontSize='2rem' />
        <Typography variant='h6'>{data.label}</Typography>
      </CardContent>
    </PlaceholderNode>
  )
}

export default memo(IOPlaceholderNode)
