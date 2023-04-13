import { ReactNode } from 'react'
import Link from 'next/link'
import Image from 'next/image'
import Button from '@mui/material/Button'
import { styled } from '@mui/material/styles'
import Typography from '@mui/material/Typography'
import Box, { BoxProps } from '@mui/material/Box'
import BlankLayout from 'src/@core/layouts/BlankLayout'
import FooterIllustrations from 'src/layouts/misc/FooterIllustrations'

import Img401 from 'public/images/pages/401.png'


const BoxWrapper = styled(Box)<BoxProps>(({ theme }) => ({
  [theme.breakpoints.down('md')]: {
    width: '90vw'
  }
}))

const Img = styled(Image)(({ theme }) => ({
  marginBottom: theme.spacing(10),
  [theme.breakpoints.down('lg')]: {
    height: 450,
    marginTop: theme.spacing(10)
  },
  [theme.breakpoints.down('md')]: {
    height: 400
  },
  [theme.breakpoints.up('lg')]: {
    marginTop: theme.spacing(13)
  }
}))

const Error401 = () => {
  return (
    <Box className='content-center'>
      <Box sx={{ p: 5, display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center' }}>
        <BoxWrapper>
          <Typography variant='h1'>401</Typography>
          <Typography variant='h5' sx={{ mb: 1, fontSize: '1.5rem !important' }}>
            You are not authorized! 🔐
          </Typography>
          <Typography variant='body2'>You don&prime;t have permission to access this page.</Typography>
        </BoxWrapper>
        <Img height='487' alt='error-illustration' src={Img401} />
        <Button component={Link} href='/' variant='contained' sx={{ px: 5.5 }}>
          Back to Home
        </Button>
      </Box>
      <FooterIllustrations />
    </Box>
  )
}

Error401.getLayout = (page: ReactNode) => <BlankLayout>{page}</BlankLayout>

export default Error401
