'use client'

import { useState } from 'react'
import IconHelpCircle from '~icons/bx/help-circle'

import { Avatar, Box, Button, Card, Link, Modal, Typography } from '@mui/material'

const WelcomeCard = (props: { setCard: (card: number) => void }) => (
  <Card sx={{ p: 4, display: 'flex', flexDirection: 'column', gap: 4, alignItems: 'center' }}>
    <Avatar sx={{ width: 50, height: 50, mb: 2.25 }}>
      <IconHelpCircle fontSize='2rem' />
    </Avatar>
    <Typography variant='h5' sx={{ textAlign: 'center' }}>
      <Typography color='silver' variant='h5' component='span'>
        Welcome to the
      </Typography>{' '}
      Feldera Continuous Analytics Platform
    </Typography>
    <Box sx={{ width: '100%', display: 'flex', justifyContent: 'space-evenly' }}>
      <Button onClick={() => props.setCard(1)}>Watch introduction</Button>
      <Button href='https://www.feldera.com/docs/tour/' target='_blank' rel='noreferrer'>
        Take the tour
      </Button>
      <Button href='/demos/' LinkComponent={Link}>
        Try the demos
      </Button>
    </Box>
  </Card>
)

export const WelcomeTile = () => {
  const [card, setCard] = useState(0)
  return (
    <>
      <WelcomeCard setCard={setCard}></WelcomeCard>
      <Modal
        open={card === 1}
        onClose={() => setCard(0)}
        aria-labelledby='modal-modal-title'
        aria-describedby='modal-modal-description'
      >
        <Box
          sx={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            width: '80%',
            height: '80%',
            backgroundColor: 'black'
          }}
        >
          <iframe
            width='100%'
            height='100%'
            src='https://www.youtube.com/embed/tMLg9NyM3xk'
            style={{ borderStyle: 'none' }}
            title='YouTube video player'
            allow='accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share'
            allowFullScreen
          ></iframe>
        </Box>
      </Modal>
    </>
  )
}
