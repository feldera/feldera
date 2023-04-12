// A loading screen that can be used to show a loading spinner and the logo
// while data is being fetched on initial page loads.

import Box, { BoxProps } from '@mui/material/Box'
import CircularProgress from '@mui/material/CircularProgress'

const LoadingScreen = ({ sx }: { sx?: BoxProps['sx'] }) => {
  return (
    <Box
      sx={{
        height: '100vh',
        display: 'flex',
        alignItems: 'center',
        flexDirection: 'column',
        justifyContent: 'center',
        ...sx
      }}
    >
      <img src='/images/dbsp-primary-main.svg' alt='Logo' width='35%' height='35%' />
      <CircularProgress disableShrink sx={{ mt: 6 }} />
    </Box>
  )
}

export default LoadingScreen
