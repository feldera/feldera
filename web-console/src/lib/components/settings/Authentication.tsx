'use client'

import { ApiKeyList } from '$lib/components/settings/ApiKeyList'
import { useAuth } from '$lib/compositions/auth/useAuth'
import { GridItems } from 'src/lib/components/common/GridItems'

import { Card, CardContent, Grid, Typography } from '@mui/material'

export const AuthenticationSettings = () => {
  const { auth } = useAuth()
  return auth === 'NoAuth' ? (
    <></>
  ) : (
    <>
      <Typography variant='h6'>Authentication</Typography>
      <Card>
        <CardContent>
          <Grid container spacing={4}>
            <GridItems xs={12} sm={6}>
              <ApiKeyList></ApiKeyList>
            </GridItems>
          </Grid>
        </CardContent>
      </Card>
    </>
  )
}
