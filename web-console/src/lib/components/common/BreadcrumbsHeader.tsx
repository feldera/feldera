import PageHeader from '$lib/components/layouts/pageHeader'
import { ReactNode } from 'react'
import IconChevronRight from '~icons/bx/chevron-right'

import { Breadcrumbs } from '@mui/material'

export const BreadcrumbsHeader = (props: { children: ReactNode }) => (
  <PageHeader
    title={
      <Breadcrumbs separator={<IconChevronRight fontSize={20} />} aria-label='breadcrumb'>
        {props.children}
      </Breadcrumbs>
    }
  />
)
