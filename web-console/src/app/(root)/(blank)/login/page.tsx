'use client'

import { redirect } from 'next/navigation'

import { Authenticator } from '@aws-amplify/ui-react'
import { Box } from '@mui/material'

export default () => {
  return (
    <Box sx={{ display: 'flex', height: '100vh', justifyContent: 'center' }}>
      <Authenticator>
        {({}) => {
          redirect('/home')
        }}
      </Authenticator>
    </Box>
  )
}
