import '@emotion/react'
import { Palette } from '@mui/material'

declare module '@emotion/react' {
  export interface Theme {
    palette: Palette
  }
}
