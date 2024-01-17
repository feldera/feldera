'use client'

import { TableSqlPrograms } from '$lib/components/analytics/TableSqlPrograms'
import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'

import { Link } from '@mui/material'

const SqlPrograms = () => {
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/analytics/programs`} data-testid='button-breadcrumb-sql-programs'>
          SQL Programs
        </Link>
      </BreadcrumbsHeader>
      <TableSqlPrograms />
    </>
  )
}

export default SqlPrograms
