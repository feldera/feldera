import { ReactNode } from 'react'
import Link from 'next/link'
import Image from 'next/image'
import Box, { BoxProps } from '@mui/material/Box'
import { styled } from '@mui/material/styles'
import Typography, { TypographyProps } from '@mui/material/Typography'
import { Settings } from 'src/@core/context/settingsContext'
import themeConfig from 'src/configs/themeConfig'
import darkLogo from 'public/images/dbsp-primary-dark.svg'
import lightLogo from 'public/images/dbsp-primary-main.svg'

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

const HeaderTitle = styled(Typography)<TypographyProps>(({ theme }) => ({
  fontWeight: 600,
  lineHeight: 'normal',
  textTransform: 'uppercase',
  color: theme.palette.text.primary,
  transition: 'opacity .25s ease-in-out, margin .25s ease-in-out'
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
    <MenuHeaderWrapper className='nav-header' sx={{ pl: 6 }}>
      {userVerticalNavMenuBranding ? (
        userVerticalNavMenuBranding(props)
      ) : (
        <StyledLink href='/' passHref>
          <Image src={logo} alt='Logo' width={30} height={25} />
          <HeaderTitle variant='h6' sx={{ ml: 3 }}>
            {themeConfig.templateName}
          </HeaderTitle>
        </StyledLink>
      )}
    </MenuHeaderWrapper>
  )
}

export default VerticalNavHeader
