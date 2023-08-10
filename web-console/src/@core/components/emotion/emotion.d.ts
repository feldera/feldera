// Ensures we have correct type information for the palette if we access it
// within a styled component.
//
// (e.g. something like styled.div`color: ${props =>
// props.theme.palette.primary.main}` will type check correctly)

import '@emotion/react'
import { Palette } from '@mui/material'

declare module '@emotion/react' {
  export interface Theme {
    palette: Palette
  }
}
