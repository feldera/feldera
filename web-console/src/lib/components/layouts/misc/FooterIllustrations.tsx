'use client'

import Image from 'next/image'
import { Fragment } from 'react'

import { styled, useTheme } from '@mui/material/styles'
import useMediaQuery from '@mui/material/useMediaQuery'

// Styled Components
const MaskImg = styled(Image)(() => ({
  bottom: 0,
  zIndex: -1,
  width: '100%',
  position: 'absolute'
}))

const FooterIllustrations = () => {
  const theme = useTheme()

  const hidden = useMediaQuery(theme.breakpoints.down('md'))
  const mask = theme.palette.mode === 'light' ? '/images/pages/misc-mask-light.png' : '/images/pages/misc-mask-dark.png'

  if (!hidden) {
    return (
      <Fragment>
        <MaskImg alt='mask' src={mask} priority />
      </Fragment>
    )
  } else {
    return null
  }
}

export default FooterIllustrations
