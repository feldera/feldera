import { Theme } from '@mui/material/styles'

const GlobalStyles = (theme: Theme) => {
  return {
    '.ps__rail-y': {
      zIndex: 1,
      right: '0 !important',
      left: 'auto !important',
      '&:hover, &:focus, &.ps--clicking': {
        backgroundColor:
          theme.palette.mode === 'light' ? 'rgba(86, 106, 127, 0.1) !important' : 'rgba(203, 203, 226, 0.15) !important'
      },
      '& .ps__thumb-y': {
        right: '3px !important',
        left: 'auto !important',
        backgroundColor:
          theme.palette.mode === 'light' ? 'rgba(86, 106, 127, 0.25) !important' : 'rgba(203, 203, 226, 0.4) !important'
      },
      '.layout-vertical-nav &': {
        '& .ps__thumb-y': {
          width: 4,
          backgroundColor:
            theme.palette.mode === 'light'
              ? 'rgba(86, 106, 127, 0.25) !important'
              : 'rgba(203, 203, 226, 0.4) !important'
        },
        '&:hover, &:focus, &.ps--clicking': {
          backgroundColor: 'transparent !important',
          '& .ps__thumb-y': {
            width: 6
          }
        }
      }
    },

    '#nprogress': {
      pointerEvents: 'none',
      '& .bar': {
        left: 0,
        top: 0,
        height: 3,
        width: '100%',
        zIndex: 2000,
        position: 'fixed',
        backgroundColor: theme.palette.primary.main
      }
    }
  }
}

export default GlobalStyles
