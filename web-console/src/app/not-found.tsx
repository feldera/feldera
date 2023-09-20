'use client'

import FooterIllustrations from '$lib/components/layouts/misc/FooterIllustrations'
import Link from 'next/link'
import BlankLayout from 'src/@core/layouts/BlankLayout'

import Box, { BoxProps } from '@mui/material/Box'
import Button from '@mui/material/Button'
import { styled } from '@mui/material/styles'
import Typography from '@mui/material/Typography'

const BoxWrapper = styled(Box)<BoxProps>(({ theme }) => ({
  [theme.breakpoints.down('md')]: {
    width: '90vw'
  }
}))

const Error404 = () => {
  return (
    <BlankLayout>
      <Box className='content-center'>
        <Box sx={{ p: 5, display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center' }}>
          <BoxWrapper sx={{ p: 10 }}>
            <Typography variant='h1'>404</Typography>
            <Typography variant='h5' sx={{ mb: 1, fontSize: '1.5rem !important' }}>
              Page Not Found ⚠️
            </Typography>
            <Typography variant='body2'>We couldn&prime;t find the page you are looking for.</Typography>
          </BoxWrapper>
          <Button component={Link} href='/' variant='contained' sx={{ px: 5.5 }}>
            Back to Home
          </Button>
        </Box>
        <FooterIllustrations />
      </Box>
    </BlankLayout>
  )
}

export default Error404
