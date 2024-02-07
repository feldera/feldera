'use client'

import { TableSqlPrograms } from '$lib/components/analytics/TableSqlPrograms'
import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'

const SqlPrograms = () => {
  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/analytics/programs`} data-testid='button-breadcrumb-sql-programs'>
          SQL Programs
        </Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <TableSqlPrograms />
    </>
  )
}

export default SqlPrograms
