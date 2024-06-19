'use client'

import { Pipelines } from '$lib/components/home/Pipelines'
import { WelcomeTile } from '$lib/components/home/WelcomeCard'

import Grid from '@mui/material/Grid'

const Home = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <Grid item xs={6}>
        <WelcomeTile></WelcomeTile>
      </Grid>
      <Grid item xs={6}>
        <></>
      </Grid>
      <Grid item xs={6}>
        <Pipelines />
      </Grid>
    </Grid>
  )
}

export default Home
