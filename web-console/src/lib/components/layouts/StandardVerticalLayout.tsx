'use client'

import VerticalNavItems from '$lib/functions/navigation/vertical'
import { ReactNode, useState } from 'react'
import { useSettings } from 'src/@core/hooks/useSettings'
import VerticalLayout from 'src/@core/layouts/VerticalLayout'
import { useAuthentication } from 'src/lib/compositions/useAuth'

import { ClickAwayListener } from '@mui/base'
import { Box, Collapse, Grid, Typography } from '@mui/material'
import { Theme } from '@mui/material/styles'
import useMediaQuery from '@mui/material/useMediaQuery'

import { AccountButton } from './AccountButton'
import { AccountPanel } from './AccountPanel'
import { GlobalDialog } from './GlobalDialog'
import VerticalAppBarContent from './vertical/AppBarContent'

interface Props {
  children: ReactNode
}

const AccountGroup = () => {
  const [expand, setExpand] = useState(false)
  const user = useAuthentication()
  if (!user)
    return (
      <>
        <Typography sx={{ color: 'GrayText', textAlign: 'center' }}>not authenticated</Typography>
      </>
    )
  return (
    <ClickAwayListener
      onClickAway={() => {
        setExpand(false)
      }}
    >
      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
        <Collapse in={expand} sx={{ width: '100%' }}>
          <AccountPanel></AccountPanel>
        </Collapse>
        <AccountButton onClick={() => setExpand(!expand)}></AccountButton>
      </Box>
    </ClickAwayListener>
  )
}

const StandardVerticalLayout = ({ children }: Props) => {
  const { settings, saveSettings } = useSettings()

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
      verticalNavItems={VerticalNavItems()} // Navigation Items
      afterVerticalNavMenuContent={AccountGroup}
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
      <Grid container spacing={6} className='match-height'>
        {children}
      </Grid>
      <GlobalDialog />
    </VerticalLayout>
  )
}

export default StandardVerticalLayout
