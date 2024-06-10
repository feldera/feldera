import { ThemeColor } from '@core/layouts/types'
import { PaletteMode } from '@mui/material'

const DefaultPalette = (mode: PaletteMode, themeColor: ThemeColor) => {
  const whiteColor = '#FFFFFF'
  const lightColor = '#32475c'
  const darkColor = '#dbdbeb'
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
      black: '#000000',
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
      light: mode === 'light' ? '#FFD1C9' : '#731100',
      main: '#FF3E1D',
      dark: '#E8381A',
      contrastText: whiteColor
    },
    warning: {
      light: '#FFB826',
      main: mode === 'light' ? '#FFAB00' : '#FFAB00',
      dark: '#E89C00',
      contrastText: mode === 'light' ? '#000000' : '#000000'
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
      primary: mainColor + 'DF', // 0.87
      secondary: mainColor + '99', // 0.6
      disabled: mainColor + '61' // 0.38
    },
    divider: mainColor + '1F', // 0.12
    background: {
      paper: mode === 'light' ? whiteColor : darkPaperBgColor,
      default: mode === 'light' ? '#F5F5F9' : '#232333'
    },
    action: {
      active: mainColor + '8A', // 0.54
      hover: mainColor + '0A', // 0.04
      selected: mainColor + '14', // 0.08
      disabled: mainColor + '43', // 0.26
      disabledBackground: mainColor + '1F', // 0.12
      focus: mainColor + '1F' // 0.12
    }
  }
}

export default DefaultPalette
