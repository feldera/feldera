import { ReactElement } from 'react'

import Avatar from '@mui/material/Avatar'
import Box from '@mui/material/Box'
import Typography from '@mui/material/Typography'

export interface TabLabelProps {
  title: string
  active: boolean
  subtitle: string
  icon: ReactElement
}

const TabLabel = (props: TabLabelProps) => {
  const { icon, title, subtitle, active } = props

  return (
    <div>
      <Box sx={{ display: 'flex', alignItems: 'center' }}>
        <Avatar
          variant='rounded'
          sx={{
            mr: 3,
            ...(active
              ? { color: 'common.white', backgroundColor: 'primary.main' }
              : { backgroundColor: 'action.selected' })
          }}
        >
          {icon}
        </Avatar>
        <Box sx={{ textAlign: 'left' }}>
          <Typography>{title}</Typography>
          <Typography variant='caption' sx={{ textTransform: 'none' }}>
            {subtitle}
          </Typography>
        </Box>
      </Box>
    </div>
  )
}

export default TabLabel
