import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'

import PageHeader from 'src/layouts/components/page-header'
import DataSourceTable from 'src/connectors/DataSourceTable'
import { usePageHeader } from 'src/compositions/ui/pageHeader'

const SqlPrograms = () => {
  usePageHeader(s => s.setHeader)({ title: 'Connectors', subtitle: 'View and edit data sources.' })
  return (
    <Grid container spacing={6} className='match-height'>
      <Grid item xs={12}>
        <DataSourceTable />
      </Grid>
    </Grid>
  )
}

export default SqlPrograms
