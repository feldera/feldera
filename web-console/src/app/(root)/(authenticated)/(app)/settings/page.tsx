'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import { AuthenticationSettings } from 'src/lib/components/settings/Authentication'

import Stack from '@mui/material/Stack'

export default function () {
  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/settings/`}>Settings</Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <Stack sx={{ width: '100%', marginLeft: 0 }} spacing={2}>
        <AuthenticationSettings></AuthenticationSettings>
      </Stack>
    </>
  )
}
