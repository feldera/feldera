'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import DataSourceTable from '$lib/components/connectors/DataSourceTable'

import { Link } from '@mui/material'

const SqlPrograms = () => {
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/connectors/list`}>Connectors</Link>
      </BreadcrumbsHeader>
      <DataSourceTable />
    </>
  )
}

export default SqlPrograms
