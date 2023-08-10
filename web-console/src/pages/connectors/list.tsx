import DataSourceTable from '$lib/components/connectors/DataSourceTable'
import { usePageHeader } from '$lib/compositions/global/pageHeader'

import Grid from '@mui/material/Grid'

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
