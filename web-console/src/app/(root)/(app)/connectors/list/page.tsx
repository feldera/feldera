'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import DataSourceTable from '$lib/components/connectors/DataSourceTable'

import { Link } from '@mui/material'
import Grid from '@mui/material/Grid'

const SqlPrograms = () => {
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/connectors/list`}>Connectors</Link>
      </BreadcrumbsHeader>
      <Grid item xs={12}>
        <DataSourceTable />
      </Grid>
    </>
  )
}

export default SqlPrograms
