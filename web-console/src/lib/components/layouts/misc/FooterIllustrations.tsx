'use client'

import Image from 'next/image'

import { useTheme } from '@mui/material/styles'
import useMediaQuery from '@mui/material/useMediaQuery'

const FooterIllustrations = () => {
  const theme = useTheme()

  const hidden = useMediaQuery(theme.breakpoints.down('md'))
  const mask = theme.palette.mode === 'light' ? '/images/pages/misc-mask-light.png' : '/images/pages/misc-mask-dark.png'

  if (hidden) {
    return null
  }
  return (
    <>
      <Image
        alt='mask'
        src={mask}
        fill={true}
        style={{
          zIndex: -1,
          position: 'absolute'
        }}
        priority
      />
    </>
  )
}

export default FooterIllustrations
