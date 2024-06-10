import themeConfig from '$lib/functions/configs/themeConfig'
import Link from 'next/link'
import { usePathname } from 'next/navigation'

import { Settings } from '@core/context/settingsTypes'
import { NavLink } from '@core/layouts/types'
import Box, { BoxProps } from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import ListItem from '@mui/material/ListItem'
import ListItemButton from '@mui/material/ListItemButton'
import ListItemIcon from '@mui/material/ListItemIcon'
import { styled, useTheme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'

interface Props {
  item: NavLink
  settings: Settings
  navVisible?: boolean
  toggleNavVisibility: () => void
}

const MenuItemTextMetaWrapper = styled(Box)<BoxProps>({
  width: '100%',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  transition: 'opacity .25s ease-in-out',
  ...(themeConfig.menuTextTruncate && { overflow: 'hidden' })
})

const VerticalNavLink = ({ item, navVisible, toggleNavVisibility }: Props) => {
  const pathname = usePathname()
  const isNavLinkActive = () =>
    (Array.isArray(item.path) ? item.path : [item.path]).find(path => path && pathname.startsWith(path) && path !== '/')
  const theme = useTheme()
  return (
    <ListItem disablePadding sx={{ mt: 1.5, px: '0 !important' }}>
      <ListItemButton
        LinkComponent={Link}
        disabled={item.disabled || false}
        href={item.path === undefined ? '/' : typeof item.path === 'string' ? item.path : item.path[0]}
        data-testid={item.testid}
        sx={{
          width: '100%',
          borderRadius: '0 100px 100px 0',
          color: theme.palette.text.primary,
          py: 1,
          transition: 'opacity .25s ease-in-out',
          '&.active, &.active:hover': {
            boxShadow: theme.shadows[3],
            backgroundImage: `linear-gradient(98deg, ${theme.palette.customColors.primaryGradient}, ${theme.palette.primary.main} 94%)`
          },
          '&.active .MuiTypography-root, &.active .MuiSvgIcon-root': {
            color: `${theme.palette.common.white} !important`
          },

          pl: 5.5,
          ...(item.disabled ? { pointerEvents: 'none' } : { cursor: 'pointer' })
        }}
        className={isNavLinkActive() ? 'active' : ''}
        {...(item.openInNewTab ? { target: '_blank', rel: 'noreferrer' } : null)}
        onClick={e => {
          if (item.path === undefined) {
            e.preventDefault()
            e.stopPropagation()
          }
          if (navVisible) {
            toggleNavVisibility()
          }
        }}
      >
        <ListItemIcon
          sx={{
            mr: 2.5,
            color: 'text.primary',
            transition: 'margin .25s ease-in-out'
          }}
          style={{ fontSize: '1.5rem' }}
        >
          {item.icon}
        </ListItemIcon>

        <MenuItemTextMetaWrapper>
          <Typography {...(themeConfig.menuTextTruncate && { noWrap: true })}>{item.title}</Typography>
          {item.badgeContent ? (
            <Chip
              label={item.badgeContent}
              color={item.badgeColor || 'primary'}
              sx={{
                height: 20,
                fontWeight: 500,
                marginLeft: 1.25,
                '& .MuiChip-label': { px: 1.5, textTransform: 'capitalize' }
              }}
            />
          ) : null}
        </MenuItemTextMetaWrapper>
      </ListItemButton>
    </ListItem>
  )
}

export default VerticalNavLink
