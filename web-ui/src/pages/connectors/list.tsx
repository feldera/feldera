import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'

import PageHeader from 'src/layouts/components/page-header'
import DataSourceTable from 'src/connectors/DataSourceTable'

const SqlPrograms = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <PageHeader
        title={<Typography variant='h5'>Data Sources</Typography>}
        subtitle={<Typography variant='body2'>View status and edit already data sources.</Typography>}
      />

      <Grid item xs={12}>
        <DataSourceTable />
      </Grid>
    </Grid>
  )
}

export default SqlPrograms
