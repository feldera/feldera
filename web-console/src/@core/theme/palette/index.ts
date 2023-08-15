import { PaletteMode } from '@mui/material'
import { ThemeColor } from 'src/@core/layouts/types'

const DefaultPalette = (mode: PaletteMode, themeColor: ThemeColor) => {
  const whiteColor = '#FFF'
  const lightColor = '50, 71, 92'
  const darkColor = '219, 219, 235'
  const darkPaperBgColor = '#2B2C40'

  const mainColor = mode === 'light' ? lightColor : darkColor

  const primaryGradient = () => {
    if (themeColor === 'primary') {
      return '#8082FF'
    } else if (themeColor === 'secondary') {
      return '#97A2B1'
    } else if (themeColor === 'success') {
      return '#86E255'
    } else if (themeColor === 'error') {
      return '#FF5B3F'
    } else if (themeColor === 'warning') {
      return '#FFB826'
    } else {
      return '#6ACDFF'
    }
  }

  return {
    mode: mode,
    customColors: {
      main: mainColor,
      primaryGradient: primaryGradient(),
      tableHeaderBg: mode === 'light' ? '#F9FAFC' : '#3D3759'
    },
    common: {
      black: '#000',
      white: whiteColor
    },
    primary: {
      light: '#8082FF',
      main: '#696CFF',
      dark: '#6062E8',
      contrastText: whiteColor
    },
    secondary: {
      light: '#97A2B1',
      main: '#8592A3',
      dark: '#798594',
      contrastText: whiteColor
    },
    error: {
      light: '#FF5B3F',
      main: '#FF3E1D',
      dark: '#E8381A',
      contrastText: whiteColor
    },
    warning: {
      light: '#FFB826',
      main: '#FFAB00',
      dark: '#E89C00',
      contrastText: whiteColor
    },
    info: {
      light: '#29CCEF',
      main: '#03C3EC',
      dark: '#03B1D7',
      contrastText: whiteColor
    },
    success: {
      light: '#86E255',
      main: '#71DD37',
      dark: '#67C932',
      contrastText: whiteColor
    },
    grey: {
      50: '#FAFAFA',
      100: '#F5F5F5',
      200: '#EEEEEE',
      300: '#E0E0E0',
      400: '#BDBDBD',
      500: '#9E9E9E',
      600: '#757575',
      700: '#616161',
      800: '#424242',
      900: '#212121',
      A100: '#F5F5F5',
      A200: '#EEEEEE',
      A400: '#BDBDBD',
      A700: '#616161'
    },
    text: {
      primary: `rgba(${mainColor}, 0.87)`,
      secondary: `rgba(${mainColor}, 0.6)`,
      disabled: `rgba(${mainColor}, 0.38)`
    },
    divider: `rgba(${mainColor}, 0.12)`,
    background: {
      paper: mode === 'light' ? whiteColor : darkPaperBgColor,
      default: mode === 'light' ? '#F5F5F9' : '#232333'
    },
    action: {
      active: `rgba(${mainColor}, 0.54)`,
      hover: `rgba(${mainColor}, 0.04)`,
      selected: `rgba(${mainColor}, 0.08)`,
      disabled: `rgba(${mainColor}, 0.26)`,
      disabledBackground: `rgba(${mainColor}, 0.12)`,
      focus: `rgba(${mainColor}, 0.12)`
    }
  }
}

export default DefaultPalette
