import PageHeader from '$lib/components/layouts/pageHeader'
import { ReactNode } from 'react'

import { Breadcrumbs as MUIBreadcrumbs, Link, LinkProps, useTheme } from '@mui/material'

export const Breadcrumbs = {
  Header: (props: { children: ReactNode }) => (
    <PageHeader
      title={
        <MUIBreadcrumbs
          separator={
            // <i className={`bx bx-chevron-right`} style={{ fontSize: 20 }} />
            <i className={`bx bx-chevron-right`} style={{ fontSize: 20 }} />
          }
          aria-label='breadcrumb'
        >
          {props.children}
        </MUIBreadcrumbs>
      }
    />
  ),
  Link: (props: LinkProps) => {
    const theme = useTheme()
    return <Link color={theme.palette.text.primary} fontSize={20} fontWeight={300} {...props} />
  }
}
