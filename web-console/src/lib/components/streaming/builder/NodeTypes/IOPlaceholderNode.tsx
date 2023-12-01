// The placeholder for inputs, opens the drawer to add input connectors on a
// click.

import React, { memo } from 'react'
import { NodeProps } from 'reactflow'

import { CardContent, Link, Typography } from '@mui/material'

import { PlaceholderNode } from './'

const IOPlaceholderNode = ({ id, data }: NodeProps) => {
  return (
    <PlaceholderNode>
      <Link
        href={`#${id === 'inputPlaceholder' ? 'add_input' : 'add_output'}`}
        data-testid={id === 'inputPlaceholder' ? 'button-builder-add-input' : 'button-builder-add-output'}
      >
        <CardContent sx={{ textAlign: 'center' }}>
          {(Icon => (
            <Icon fontSize='2rem'></Icon>
          ))(data.icon)}
          <Typography variant='h6'>{data.label}</Typography>
        </CardContent>
      </Link>
    </PlaceholderNode>
  )
}

export default memo(IOPlaceholderNode)
