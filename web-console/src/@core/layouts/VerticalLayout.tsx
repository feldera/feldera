'use client'

import themeConfig from '$lib/functions/configs/themeConfig'
import ArrowUp from 'mdi-material-ui/ArrowUp'
import { useState } from 'react'

import ScrollToTop from '@core/components/scroll-to-top'
import { LayoutProps } from '@core/layouts/types'
import Box from '@mui/material/Box'
import Fab from '@mui/material/Fab'
import { styled, useTheme } from '@mui/material/styles'
import useMediaQuery from '@mui/material/useMediaQuery'

import Footer from './components/shared-components/footer'
import AppBar from './components/vertical/appBar'
import Navigation from './components/vertical/navigation'

import type { Theme } from '@mui/material/styles'

const ContentWrapper = styled('main')(({ theme }) => ({
  flexGrow: 1,
  width: '100%',
  paddingLeft: theme.spacing(6),
  paddingRight: theme.spacing(6),
  transition: 'padding .25s ease-in-out',
  [theme.breakpoints.down('sm')]: {
    paddingLeft: theme.spacing(4),
    paddingRight: theme.spacing(4)
  }
}))

export const VerticalLayout = (props: LayoutProps) => {
  const { settings, children, scrollToTop } = props

  const { contentWidth } = settings
  const navWidth = themeConfig.navigationSize

  const [navVisible, setNavVisible] = useState<boolean>(false)
  const theme = useTheme()
  const fixedFooter = useMediaQuery((theme: Theme) => theme.breakpoints.up('md'))

  const toggleNavVisibility = () => setNavVisible(!navVisible)

  return (
    <>
      <Box
        sx={{
          height: '100%',
          width: '100vw', // Ignore the global vertical scrollbar
          display: 'flex'
        }}
      >
        <Navigation
          navWidth={navWidth}
          navVisible={navVisible}
          setNavVisible={setNavVisible}
          toggleNavVisibility={toggleNavVisibility}
          {...props}
        />
        <Box
          className='layout-content-wrapper'
          sx={{
            backgroundColor: theme.palette.background.default,
            flexGrow: 1,
            minWidth: 0,
            display: 'flex',
            minHeight: '100vh',
            flexDirection: 'column'
          }}
        >
          <AppBar toggleNavVisibility={toggleNavVisibility} {...props} />

          <ContentWrapper
            className='layout-page-content'
            sx={{
              ...(contentWidth === 'boxed' && {
                mx: 'auto',
                pb: fixedFooter ? '5rem' : undefined,
                '@media (min-width:1440px)': { maxWidth: 1440 },
                '@media (min-width:1200px)': { maxWidth: '100%' }
              })
            }}
          >
            {children}
          </ContentWrapper>
          <Footer {...props} />
        </Box>
      </Box>

      {scrollToTop ? (
        scrollToTop(props)
      ) : (
        <ScrollToTop className='mui-fixed'>
          <Fab color='primary' size='small' aria-label='scroll back to top'>
            <ArrowUp />
          </Fab>
        </ScrollToTop>
      )}
    </>
  )
}
