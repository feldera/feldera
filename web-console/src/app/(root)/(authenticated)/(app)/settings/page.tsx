import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import { AuthenticationSettings } from 'src/lib/components/settings/Authentication'

import { Link } from '@mui/material'
import Stack from '@mui/material/Stack'

export default function () {
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/settings/`}>Settings</Link>
      </BreadcrumbsHeader>
      <Stack sx={{ width: '100%', marginLeft: 0 }} spacing={2}>
        <AuthenticationSettings></AuthenticationSettings>
      </Stack>
    </>
  )
}
