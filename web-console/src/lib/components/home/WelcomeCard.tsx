import { Button, Card, Typography, Box, Avatar, Collapse, IconButton } from '@mui/material'
import { Icon } from '@iconify/react'
import { useState } from 'react'

const WelcomeCard = (props: { setCard: (card: number) => void }) => (
  <Card sx={{ p: 4, display: 'flex', flexDirection: 'column', gap: 4, alignItems: 'center' }}>
    <Avatar sx={{ width: 50, height: 50, mb: 2.25 }}>
      <Icon icon='bx:help-circle' fontSize='2rem' />
    </Avatar>
    <Typography variant='h5' sx={{ textAlign: 'center' }}>
      <Typography color='silver' variant='h5' component='span'>
        Welcome to the
      </Typography>{' '}
      Feldera Continuous Analytics Platform
    </Typography>
    <Box sx={{ width: '100%', display: 'flex', justifyContent: 'space-evenly' }}>
      <Button onClick={() => props.setCard(1)}>Watch introduction</Button>
      <Button href='https://docs.feldera.io/docs/tour/' target='_blank' rel='noreferrer'>
        Take the tour
      </Button>
      <Button href='https://docs.feldera.io/docs/demos/' target='_blank' rel='noreferrer'>
        Follow the demos
      </Button>
    </Box>
  </Card>
)

const VideoCard = (props: { setCard: (card: number) => void }) => (
  <Box sx={{ position: 'relative' }}>
    <iframe
      width='100%'
      height='490'
      src='https://www.youtube.com/embed/iT4k5DCnvPU'
      title='YouTube video player'
      frameborder='0'
      allow='accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share'
      allowfullscreen
    ></iframe>
    <Box
      sx={{ position: 'absolute', left: '0', top: '0', p: 0.5, m: 1.8, backgroundColor: 'primary.contrastText' }}
      onClick={() => props.setCard(0)}
    >
      <IconButton size='small' sx={{ m: 0 }}>
        <Icon icon='bx:x' fontSize={36} />
      </IconButton>
    </Box>
  </Box>
)

export const WelcomeTile = () => {
  const [card, setCard] = useState(0)
  return (
    <>
      <Collapse in={card == 0} orientation='vertical'>
        <WelcomeCard setCard={setCard}></WelcomeCard>
      </Collapse>
      <Collapse in={card == 1} orientation='vertical'>
        <VideoCard setCard={setCard}></VideoCard>
      </Collapse>
    </>
  )
}
