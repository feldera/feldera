'use client'

import { useAuth } from '$lib/compositions/auth/useAuth'
import VerticalNavItems from '$lib/functions/navigation/vertical'
import { ReactNode } from 'react'

import { useSettings } from '@core/hooks/useSettings'
import { VerticalLayout } from '@core/layouts/VerticalLayout'
import { Theme } from '@mui/material/styles'
import useMediaQuery from '@mui/material/useMediaQuery'

import { GlobalDialog } from './GlobalDialog'
import VerticalAppBarContent from './vertical/AppBarContent'

interface Props {
  children: ReactNode
}

export const StandardVerticalLayout = ({ children }: Props) => {
  const { settings, saveSettings } = useSettings()

  const { auth } = useAuth()

  /**
   *  The below variable will hide the current layout menu at given screen size.
   *  The menu will be accessible from the Hamburger icon only (Vertical Overlay Menu).
   *  You can change the screen size from which you want to hide the current layout menu.
   *  Please refer useMediaQuery() hook: https://mui.com/components/use-media-query/,
   *  to know more about what values can be passed to this hook.
   */
  const hidden = useMediaQuery((theme: Theme) => theme.breakpoints.down('lg'))

  return (
    <VerticalLayout
      hidden={hidden}
      settings={settings}
      saveSettings={saveSettings}
      verticalNavItems={VerticalNavItems({ showSettings: auth !== 'NoAuth' })} // Navigation Items
      afterVerticalNavMenuContent={() => <></>}
      verticalAppBarContent={(
        props // AppBar Content
      ) => (
        <VerticalAppBarContent
          hidden={hidden}
          settings={settings}
          saveSettings={saveSettings}
          toggleNavVisibility={props.toggleNavVisibility}
        />
      )}
    >
      {children}
      <GlobalDialog />
    </VerticalLayout>
  )
}
