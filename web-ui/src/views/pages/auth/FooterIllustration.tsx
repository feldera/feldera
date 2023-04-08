// ** React Imports
import { Fragment, ReactNode } from 'react'

// ** MUI Components
import useMediaQuery from '@mui/material/useMediaQuery'
import { styled, useTheme } from '@mui/material/styles'

interface FooterIllustrationsProp {
  image1?: ReactNode
  image2?: ReactNode
}

// Styled Components
const MaskImg = styled('img')(() => ({
  bottom: 0,
  zIndex: -1,
  width: '100%',
  position: 'absolute'
}))

const Tree1Img = styled('img')(() => ({
  left: 0,
  bottom: 0,
  position: 'absolute'
}))

const Tree2Img = styled('img')(() => ({
  right: 0,
  bottom: 0,
  position: 'absolute'
}))

const FooterIllustrationsV1 = (props: FooterIllustrationsProp) => {
  // ** Props
  const { image1, image2 } = props

  // ** Hook
  const theme = useTheme()

  // ** Vars
  const hidden = useMediaQuery(theme.breakpoints.down('md'))

  if (!hidden) {
    return (
      <Fragment>
        {image1 || <Tree1Img alt='tree' src='/images/pages/auth-v1-tree.png' />}
        <MaskImg alt='mask' src={`/images/pages/auth-v1-mask-${theme.palette.mode}.png`} />
        {image2 || <Tree2Img alt='tree-2' src='/images/pages/auth-v1-tree-2.png' />}
      </Fragment>
    )
  } else {
    return null
  }
}

export default FooterIllustrationsV1
