import { Fragment, ReactNode } from 'react'
import Image from 'next/image'
import useMediaQuery from '@mui/material/useMediaQuery'
import { styled, useTheme } from '@mui/material/styles'

import Tree2 from 'public/images/pages/tree-2.png'
import MiscMaskLight from 'public/images/pages/misc-mask-light.png'
import MiscMaskDark from 'public/images/pages/misc-mask-dark.png'

interface FooterIllustrationsProp {
  image?: ReactNode
}

// Styled Components
const MaskImg = styled(Image)(() => ({
  bottom: 0,
  zIndex: -1,
  width: '100%',
  position: 'absolute'
}))

const TreeImg = styled(Image)(({ theme }) => ({
  left: '2.25rem',
  bottom: '4.25rem',
  position: 'absolute',
  [theme.breakpoints.down('lg')]: {
    left: 0,
    bottom: 0
  }
}))

const FooterIllustrations = (props: FooterIllustrationsProp) => {
  const { image } = props
  const theme = useTheme()

  const hidden = useMediaQuery(theme.breakpoints.down('md'))
  const mask = theme.palette.mode === 'light' ? MiscMaskLight : MiscMaskDark

  if (!hidden) {
    return (
      <Fragment>
        {image || <TreeImg alt='tree' src={Tree2} />}
        <MaskImg alt='mask' src={mask} />
      </Fragment>
    )
  } else {
    return null
  }
}

export default FooterIllustrations
