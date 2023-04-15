import Grid from '@mui/material/Grid'
import { PageHeaderProps } from './types'

const PageHeader = (props: PageHeaderProps) => {
  const { title, subtitle } = props

  return (
    <Grid item xs={12}>
      {title}
      {subtitle || null}
    </Grid>
  )
}

export default PageHeader
