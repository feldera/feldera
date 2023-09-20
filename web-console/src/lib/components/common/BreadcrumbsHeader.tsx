import PageHeader from '$lib/components/layouts/pageHeader'
import { ReactNode } from 'react'

import { Icon } from '@iconify/react'
import { Breadcrumbs } from '@mui/material'

export const BreadcrumbsHeader = (props: { children: ReactNode }) => (
  <PageHeader
    title={
      <Breadcrumbs separator={<Icon icon='bx:chevron-right' fontSize={20} />} aria-label='breadcrumb'>
        {props.children}
      </Breadcrumbs>
    }
  />
)
