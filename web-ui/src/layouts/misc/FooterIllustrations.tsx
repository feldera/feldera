import { Fragment } from 'react'
import Image from 'next/image'
import useMediaQuery from '@mui/material/useMediaQuery'
import { styled, useTheme } from '@mui/material/styles'

import MiscMaskLight from 'public/images/pages/misc-mask-light.png'
import MiscMaskDark from 'public/images/pages/misc-mask-dark.png'

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
  const mask = theme.palette.mode === 'light' ? MiscMaskLight : MiscMaskDark

  if (!hidden) {
    return (
      <Fragment>
        <MaskImg alt='mask' src={mask} />
      </Fragment>
    )
  } else {
    return null
  }
}

export default FooterIllustrations
