import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import PageHeader from 'src/layouts/components/page-header'
import TableSqlPrograms from 'src/analytics/TableSqlPrograms'
import { usePageHeader } from 'src/compositions/ui/pageTitle'

const SqlPrograms = () => {
  usePageHeader(s => s.setHeader)(<PageHeader
    title="SQL Programs"
    subtitle="View status and edit already defined SQL programs."
  />)
  return (
    <Grid container spacing={6} className='match-height'>
      <Grid item xs={12}>
        <TableSqlPrograms />
      </Grid>
    </Grid>
  )
}

export default SqlPrograms
