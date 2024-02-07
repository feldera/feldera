import PageHeader from '$lib/components/layouts/pageHeader'
import { ReactNode } from 'react'
import IconChevronRight from '~icons/bx/chevron-right'

import { Breadcrumbs as MUIBreadcrumbs, Link, LinkProps, useTheme } from '@mui/material'

export const Breadcrumbs = {
  Header: (props: { children: ReactNode }) => (
    <PageHeader
      title={
        <MUIBreadcrumbs separator={<IconChevronRight fontSize={24} />} aria-label='breadcrumb'>
          {props.children}
        </MUIBreadcrumbs>
      }
    />
  ),
  Link: (props: LinkProps) => {
    const theme = useTheme()
    return <Link color={theme.palette.text.primary} variant='h6' {...props} />
  }
}
