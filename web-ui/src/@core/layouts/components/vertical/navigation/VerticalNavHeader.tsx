import { ReactNode } from 'react'
import Link from 'next/link'
import Image from 'next/image'
import Box, { BoxProps } from '@mui/material/Box'
import { styled } from '@mui/material/styles'
import { Settings } from 'src/@core/context/settingsContext'
import darkLogo from 'public/images/feldera-primary-dark.svg'
import lightLogo from 'public/images/feldera-primary-main.svg'

interface Props {
  hidden: boolean
  settings: Settings
  toggleNavVisibility: () => void
  saveSettings: (values: Settings) => void
  verticalNavMenuBranding?: (props?: any) => ReactNode
}

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

const VerticalNavHeader = (props: Props) => {
  const { verticalNavMenuBranding: userVerticalNavMenuBranding } = props
  const logo = props.settings.mode === 'dark' ? darkLogo : lightLogo

  return (
    <MenuHeaderWrapper className='nav-header' sx={{ pl: 3 }}>
      {userVerticalNavMenuBranding ? (
        userVerticalNavMenuBranding(props)
      ) : (
        <StyledLink href='/' passHref>
          <Image src={logo} alt='Logo' width={130} height={70} />
        </StyledLink>
      )}
    </MenuHeaderWrapper>
  )
}

export default VerticalNavHeader
