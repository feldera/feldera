'use client'

import TableSqlPrograms from '$lib/components/analytics/TableSqlPrograms'
import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'

import { Link } from '@mui/material'
import Grid from '@mui/material/Grid'

const SqlPrograms = () => {
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/analytics/programs`}>SQL Programs</Link>
      </BreadcrumbsHeader>
      <Grid item xs={12}>
        <TableSqlPrograms />
      </Grid>
    </>
  )
}

export default SqlPrograms
