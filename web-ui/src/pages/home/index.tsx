import Link from 'next/link'

import Card from '@mui/material/Card'
import Grid from '@mui/material/Grid'
import Box from '@mui/material/Box'
import Typography from '@mui/material/Typography'
import CardHeader from '@mui/material/CardHeader'
import CardContent from '@mui/material/CardContent'
import Button from '@mui/material/Button'
import {Icon} from '@iconify/react'
import Avatar from '@mui/material/Avatar'
import CardActions from '@mui/material/CardActions'

import Health from 'src/home/Health'
import Pipelines from 'src/home/Pipelines'

const CardSupport = () => {
  return (
    <Card>
      <CardContent
        sx={{
          display: 'flex',
          textAlign: 'center',
          alignItems: 'center',
          flexDirection: 'column',
          p: theme => `${theme.spacing(9.75, 5, 9.25)} !important`
        }}
      >
        <Avatar sx={{ width: 50, height: 50, mb: 2.25 }}>
          <Icon icon='bx:help-circle' fontSize='2rem' />
        </Avatar>
        <Typography variant='h6' sx={{ mb: 2.75 }}>
          Welcome to dbsp!
        </Typography>
        <Typography variant='body2' sx={{ mb: 6 }}>
          Please make sure to take the tour or watch our Demo video to understand where to go from here and how to use
          dbsp.
        </Typography>
        <CardActions className='card-action-dense'>
          <Button>Take the tour</Button>
          <Button>Watch our Demo</Button>
          <Button>Read the Docs</Button>
        </CardActions>
      </CardContent>
    </Card>
  )
}

const Home = () => {
  return (
    <Grid container spacing={6}>
      <Grid item xs={7}>
        <Card>
          <CardHeader title='Kick start your project 🚀'></CardHeader>
          <CardContent>
            <Typography sx={{ mb: 2 }}>All the best for your new project.</Typography>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                '&:not(:last-of-type)': { mb: 4 },
                '& svg': { color: 'text.disabled' }
              }}
            >
              <Icon icon='bx:chevron-right' />
              <Typography sx={{ ml: 1.5, color: 'primary.main', textDecoration: 'none' }} href='#' component={Link}>
                Start by Adding Input Connectors
              </Typography>
            </Box>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                '&:not(:last-of-type)': { mb: 4 },
                '& svg': { color: 'text.disabled' }
              }}
            >
              <Icon icon='bx:chevron-right' />
              <Typography sx={{ ml: 1.5, color: 'primary.main', textDecoration: 'none' }} href='#' component={Link}>
                Write a SQL program to transform the input
              </Typography>
            </Box>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                '&:not(:last-of-type)': { mb: 4 },
                '& svg': { color: 'text.disabled' }
              }}
            >
              <Icon icon='bx:chevron-right' />
              <Typography sx={{ ml: 1.5, color: 'primary.main', textDecoration: 'none' }} href='#' component={Link}>
                Send Results to the Output Connectors
              </Typography>
            </Box>
          </CardContent>
        </Card>
      </Grid>
      <Grid item xs={5}>
        <CardSupport />
      </Grid>
      <Grid item xs={4}>
        <Health />
      </Grid>
      <Grid item xs={4}>
        <Pipelines />
      </Grid>
    </Grid>
  )
}

export default Home
