import { Button, Card, Grid, Typography, Box } from '@mui/material'

export const WelcomeCard = () => (
  <Card sx={{ p: 4, display: 'flex', flexDirection: 'column', gap: 8 }}>
    <Typography variant='h4' sx={{ textAlign: 'center' }}>
      Welcome to Feldera platofrm!
    </Typography>
    <Box sx={{ display: 'flex', justifyContent: 'space-evenly' }}>
      <Button href='https://docs.feldera.io/docs/tour/' target='_blank' rel='noreferrer'>
        Take the tour
      </Button>
      <Button href='https://docs.feldera.io/docs/demos/' target='_blank' rel='noreferrer'>
        Follow the demos
      </Button>
    </Box>
  </Card>
)
