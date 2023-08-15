import { PageHeaderProps } from '$lib/types/layouts/pageHeader'

import { Typography } from '@mui/material'
import Grid from '@mui/material/Grid'

const PageHeader = (props: PageHeaderProps) => {
  const { title, subtitle } = props

  return (
    <Grid item xs={12}>
      {typeof title === 'string' ? <Typography variant='h5'>{title}</Typography> : title}
      {<Typography variant='body2'>{subtitle}</Typography>}
    </Grid>
  )
}

export default PageHeader
