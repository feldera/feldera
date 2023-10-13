'use client'

import Health from '$lib/components/home/Health'
import Pipelines from '$lib/components/home/Pipelines'
import { WelcomeTile } from '$lib/components/home/WelcomeCard'

import Grid from '@mui/material/Grid'

const Home = () => {
  return (
    <>
      <Grid item xs={7}>
        <WelcomeTile></WelcomeTile>
      </Grid>
      <Grid item xs={5}>
        <Health />
      </Grid>
      <Grid item xs={5}>
        <Pipelines />
      </Grid>
    </>
  )
}

export default Home
