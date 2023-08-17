import DataSourceTable from '$lib/components/connectors/DataSourceTable'

import Grid from '@mui/material/Grid'

const SqlPrograms = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <Grid item xs={12}>
        <DataSourceTable />
      </Grid>
    </Grid>
  )
}

export default SqlPrograms
