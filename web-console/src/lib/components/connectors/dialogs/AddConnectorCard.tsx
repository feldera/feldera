// Show cards for adding new connectors.
//
// Also attached the dialog for the connector that opens when someone clicks
// on Add.

import { SVGImport } from '$lib/types/imports'
import Image from 'next/image'

import { Box, useTheme } from '@mui/material'
import Button from '@mui/material/Button'
import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import Typography from '@mui/material/Typography'

export const AddConnectorCard = (props: {
  id?: string
  icon: string | SVGImport
  title: string
  addInput?: { onClick: () => void } | { href: string }
  addOutput?: { onClick: () => void } | { href: string }
  'data-testid'?: string
}) => {
  const theme = useTheme()
  return (
    <Card data-testid={props['data-testid']}>
      <CardContent sx={{ textAlign: 'center' }}>
        {typeof props.icon === 'string' ? (
          <Box sx={{ height: 64, position: 'relative' }}>
            <Image
              src={props.icon}
              alt={'A connector logo'}
              fill={true}
              style={{
                fill: theme.palette.text.primary
              }}
            />
          </Box>
        ) : (
          (Icon => (
            <Icon
              style={{
                height: 64,
                objectFit: 'cover',
                width: '100%',
                fill: theme.palette.text.primary
              }}
            ></Icon>
          ))(props.icon)
        )}

        <Typography sx={{ mb: 3 }}>{props.title}</Typography>
        <Box sx={{ display: 'flex', width: '100%' }}>
          {!!props.addInput && (
            <Button variant='contained' size='small' {...props.addInput} data-testid='button-add-input'>
              Add input
            </Button>
          )}
          {!!props.addOutput && (
            <Button
              variant='contained'
              size='small'
              {...props.addOutput}
              sx={{ ml: 'auto' }}
              data-testid='button-add-output'
            >
              Add output
            </Button>
          )}
        </Box>
      </CardContent>
    </Card>
  )
}
