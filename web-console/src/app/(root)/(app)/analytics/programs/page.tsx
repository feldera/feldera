import TableSqlPrograms from '$lib/components/layouts/analytics/TableSqlPrograms'

import Grid from '@mui/material/Grid'

const SqlPrograms = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <Grid item xs={12}>
        <TableSqlPrograms />
      </Grid>
    </Grid>
  )
}

export default SqlPrograms
