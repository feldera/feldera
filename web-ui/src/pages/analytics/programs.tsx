import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import PageHeader from 'src/layouts/components/page-header'
import TableSqlPrograms from 'src/analytics/TableSqlPrograms'

const SqlPrograms = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <PageHeader
        title={<Typography variant='h5'>SQL Programs</Typography>}
        subtitle={<Typography variant='body2'>View status and edit already defined SQL programs.</Typography>}
      />

      <Grid item xs={12}>
        <TableSqlPrograms />
      </Grid>
    </Grid>
  )
}

export default SqlPrograms
