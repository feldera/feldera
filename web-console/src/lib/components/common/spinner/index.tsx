// A loading screen that can be used to show a loading spinner and the logo
// while data is being fetched on initial page loads.

import MainLogo from '$public/images/feldera/LogoSolid.svg'

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
      <MainLogo alt='Logo' width={350} height={350} />
      <CircularProgress disableShrink sx={{ mt: 6 }} />
    </Box>
  )
}

export default LoadingScreen
