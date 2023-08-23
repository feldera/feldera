import Box from '@mui/material/Box'
import Link from '@mui/material/Link'
import { Theme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'
import useMediaQuery from '@mui/material/useMediaQuery'

const FooterContent = () => {
  const hidden = useMediaQuery((theme: Theme) => theme.breakpoints.down('md'))

  return (
    <Box sx={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between' }}>
      <Typography sx={{ mr: 2 }}>{`© ${new Date().getFullYear()} Feldera`}</Typography>
      {hidden ? null : (
        <Box sx={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', '& :not(:last-child)': { mr: 4 } }}>
          <Link href='https://www.feldera.com/about-us'>About</Link>
          <Link href='mailto:learnmore@feldera.com'>Contact</Link>
          <Link href='https://www.feldera.com/docs/what-is-feldera'>Documentation</Link>
          <Link href='https://www.feldera.com/slack'>Support</Link>
        </Box>
      )}
    </Box>
  )
}

export default FooterContent
