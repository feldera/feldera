import type { ReactNode } from 'react'
import { Children } from 'react'

import { Box } from '@mui/system'

const DataGridToolbar = ({ children }: { children?: ReactNode }) => {
  return (
    <Box
      sx={{
        gap: 2,
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        justifyContent: 'space-between',
        p: theme => theme.spacing(2, 4, 2, 4)
      }}
    >
      {/* Align single item to the end (right) */}
      {Children.count(children) > 1 ? <></> : <div></div>}
      {children}
    </Box>
  )
}

export default DataGridToolbar
