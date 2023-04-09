import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import PageHeader from 'src/layouts/components/page-header'
import PipelineTable from 'src/streaming/PipelineTable'

const PipelineManagement = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <PageHeader
        title={<Typography variant='h5'>Pipeline Management</Typography>}
        subtitle={<Typography variant='body2'>Manage existing streaming pipelines.</Typography>}
      />

      <Grid item xs={12}>
        <PipelineTable />
      </Grid>
    </Grid>
  )
}

export default PipelineManagement
