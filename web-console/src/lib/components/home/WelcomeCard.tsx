import { Button, Card, Typography, Box, Avatar } from '@mui/material'
import { Icon } from '@iconify/react'

export const WelcomeCard = () => (
  <Card sx={{ p: 4, display: 'flex', flexDirection: 'column', gap: 4, alignItems: 'center' }}>
    <Avatar sx={{ width: 50, height: 50, mb: 2.25 }}>
      <Icon icon='bx:help-circle' fontSize='2rem' />
    </Avatar>
    <Typography variant='h5' sx={{ textAlign: 'center' }}>
      Welcome to the Feldera platform!
    </Typography>
    <Box sx={{ width: '100%', display: 'flex', justifyContent: 'space-evenly' }}>
      <Button href='https://docs.feldera.io/docs/tour/' target='_blank' rel='noreferrer'>
        Take the tour
      </Button>
      <Button href='https://docs.feldera.io/docs/demos/' target='_blank' rel='noreferrer'>
        Follow the demos
      </Button>
    </Box>
  </Card>
)
