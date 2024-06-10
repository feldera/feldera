import { ReactNode } from 'react'

import { Settings } from '@core/context/settingsTypes'
import Box from '@mui/material/Box'
import useMediaQuery from '@mui/material/useMediaQuery'

import FooterContent from './FooterContent'

import type { Theme } from '@mui/material/styles'
interface Props {
  settings: Settings
  saveSettings: (values: Settings) => void
  footerContent?: (props?: any) => ReactNode
}

const Footer = (props: Props) => {
  const { settings, footerContent: userFooterContent } = props
  const { contentWidth } = settings
  const fixedFooter = useMediaQuery((theme: Theme) => theme.breakpoints.up('md'))

  return (
    <Box
      component='footer'
      className='layout-footer'
      sx={{
        position: fixedFooter ? 'fixed' : undefined,
        bottom: 0,
        right: 0,
        zIndex: 10,
        width: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center'
      }}
    >
      <Box
        className='footer-content-container'
        sx={{
          display: 'flex',
          width: '100%',
          px: 6,
          py: 4,
          justifyContent: 'end',
          ...(contentWidth === 'boxed' && { '@media (min-width:1440px)': { maxWidth: 1440 } })
        }}
      >
        {userFooterContent ? userFooterContent(props) : <FooterContent />}
      </Box>
    </Box>
  )
}

export default Footer
