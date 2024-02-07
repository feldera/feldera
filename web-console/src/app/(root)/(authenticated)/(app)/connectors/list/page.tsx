'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import DataSourceTable from '$lib/components/connectors/DataSourceTable'

const SqlPrograms = () => {
  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/connectors/list`}>Connectors</Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <DataSourceTable />
    </>
  )
}

export default SqlPrograms
