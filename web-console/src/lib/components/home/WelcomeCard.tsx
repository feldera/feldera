'use client'

import { LS_PREFIX } from '$lib/types/localStorage'
import IconExpandMore from '~icons/bx/chevron-down'
import IconClose from '~icons/bx/x'

import { useLocalStorage } from '@mantine/hooks'
import { Accordion, AccordionDetails, AccordionSummary, Box, Button, Link, Typography } from '@mui/material'

const WelcomeCard = () => {
  const [welcomed, setWelcomed] = useLocalStorage({
    key: LS_PREFIX + 'home/welcomed',
    defaultValue: false
  })
  return (
    <>
      <Accordion expanded={!welcomed} onChange={() => setWelcomed(!welcomed)} disableGutters sx={{}}>
        <AccordionSummary
          expandIcon={!welcomed ? <IconClose fontSize={24} /> : <IconExpandMore fontSize={24} />}
          aria-controls='panel1-content'
          id='panel1-header'
          sx={{
            alignItems: 'start',
            p: 2,
            '.MuiAccordionSummary-content': { m: 0 },
            '.MuiAccordionSummary-expandIconWrapper': { p: 1 }
          }}
        >
          <Box sx={{ display: 'flex', flexWrap: 'wrap', width: '100%' }}>
            <Typography
              variant='h6'
              sx={{ px: 3, mr: 'auto', my: 4, display: 'flex', xs: { width: '100%' }, sm: { width: 'auto' } }}
            >
              Welcome to Feldera!
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'nowrap' }}>
              <Button
                href='https://www.feldera.com/docs/tour/'
                onClick={e => e.stopPropagation()}
                target='_blank'
                rel='noreferrer'
              >
                Take the tour
              </Button>
              <Button href='/demos/' onClick={e => e.stopPropagation()} LinkComponent={Link}>
                Try the demos
              </Button>
            </Box>
          </Box>
        </AccordionSummary>
        <AccordionDetails
          sx={{ aspectRatio: 1.77, p: 0, overflow: 'hidden', '.ytp-chrome-top ytp-show-cards-title': { width: 0 } }}
        >
          <iframe
            width='100%'
            height='100%'
            src='https://www.youtube.com/embed/tMLg9NyM3xk?showinfo=0&modestbranding=1'
            style={{ borderStyle: 'none' }}
            title='YouTube video player'
            allow='accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share'
            allowFullScreen
          ></iframe>
        </AccordionDetails>
      </Accordion>
    </>
  )
}

export const WelcomeTile = () => {
  return (
    <>
      <WelcomeCard></WelcomeCard>
    </>
  )
}
