import Grid from '@mui/material/Grid'
import TableSqlPrograms from 'src/analytics/TableSqlPrograms'
import { usePageHeader } from 'src/compositions/ui/pageHeader'

const SqlPrograms = () => {
  usePageHeader(s => s.setHeader)({
    title: 'SQL Programs',
    subtitle: 'View status and edit already defined SQL programs.'
  })
  return (
    <Grid container spacing={6} className='match-height'>
      <Grid item xs={12}>
        <TableSqlPrograms />
      </Grid>
    </Grid>
  )
}

export default SqlPrograms
