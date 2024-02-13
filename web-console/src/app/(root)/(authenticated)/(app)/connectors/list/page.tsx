'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import DataSourceTable from '$lib/components/connectors/DataSourceTable'

export default () => {
  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/connectors/list`}>Connectors</Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <DataSourceTable />
    </>
  )
}
