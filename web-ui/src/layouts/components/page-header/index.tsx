// ** MUI Imports
import Grid from '@mui/material/Grid'

// ** Types
import { PageHeaderProps } from './types'

const PageHeader = (props: PageHeaderProps) => {
  // ** Props
  const { title, subtitle } = props

  return (
    <Grid item xs={12}>
      {title}
      {subtitle || null}
    </Grid>
  )
}

export default PageHeader
