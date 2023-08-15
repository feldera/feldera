import { Children } from 'react'

import { GridFooter, GridFooterContainer, GridSlotsComponentsProps } from '@mui/x-data-grid-pro'

export function DataGridFooter({ children }: NonNullable<GridSlotsComponentsProps['footer']>) {
  return (
    <GridFooterContainer sx={{ px: 4 }}>
      {/* Align single item to the end (right) */}
      {Children.count(children) > 0 ? null : <div></div>}
      {children}
      <GridFooter
        sx={{
          border: 'none' // To delete double border.
        }}
      />
    </GridFooterContainer>
  )
}
