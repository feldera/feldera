// The placeholder for inputs, opens the drawer to add input connectors on a
// click.

import React, { memo } from 'react'
import { NodeProps } from 'reactflow'

import { Icon } from '@iconify/react'
import { CardContent, Link, Typography } from '@mui/material'

import { PlaceholderNode } from './'

const IOPlaceholderNode = ({ id, data }: NodeProps) => {
  return (
    <PlaceholderNode>
      <Link href={`#${id === 'inputPlaceholder' ? 'add_input' : 'add_output'}`}>
        <CardContent sx={{ textAlign: 'center' }}>
          <Icon icon={data.icon} fontSize='2rem' />
          <Typography variant='h6'>{data.label}</Typography>
        </CardContent>
      </Link>
    </PlaceholderNode>
  )
}

export default memo(IOPlaceholderNode)
