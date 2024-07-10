import LightLogo from '$public/images/feldera/LogoSolid.svg'
import DarkLogo from '$public/images/feldera/LogoWhite.svg'
import Link from 'next/link'
import { ReactNode } from 'react'

import { Settings } from '@core/context/settingsTypes'
import Box, { BoxProps } from '@mui/material/Box'
import { styled } from '@mui/material/styles'

const MenuHeaderWrapper = styled(Box)<BoxProps>(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  paddingRight: theme.spacing(4.5),
  transition: 'padding .25s ease-in-out',
  minHeight: theme.mixins.toolbar.minHeight
}))

const StyledLink = styled(Link)({
  display: 'flex',
  alignItems: 'center',
  textDecoration: 'none'
})

const VerticalNavHeader = (props: {
  hidden: boolean
  settings: Settings
  toggleNavVisibility: () => void
  saveSettings: (values: Settings) => void
  verticalNavMenuBranding?: (props?: any) => ReactNode
}) => {
  const { verticalNavMenuBranding: userVerticalNavMenuBranding } = props
  const Logo = props.settings.mode === 'dark' ? DarkLogo : LightLogo

  return (
    <MenuHeaderWrapper className='nav-header' sx={{ pl: 3 }}>
      {userVerticalNavMenuBranding ? (
        userVerticalNavMenuBranding(props)
      ) : (
        <StyledLink href='/' passHref>
          <Logo alt='Logo' width={130} height={70} />
        </StyledLink>
      )}
    </MenuHeaderWrapper>
  )
}

export default VerticalNavHeader
