import { ReactNode } from 'react'

import { Box, Typography } from '@mui/material'

const PageHeader = (props: { title: ReactNode; subtitle?: ReactNode }) => {
  const { title, subtitle } = props

  return (
    <Box sx={{ mt: '-4.5rem', mb: 12, pl: { xs: '3.5rem', lg: '0.5rem' } }}>
      {typeof title === 'string' ? <Typography variant='h5'>{title}</Typography> : title}
      {<Typography variant='body2'>{subtitle}</Typography>}
    </Box>
  )
}

export default PageHeader
