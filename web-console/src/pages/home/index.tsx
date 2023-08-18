import Health from '$lib/components/home/Health'
import Pipelines from '$lib/components/home/Pipelines'

import Grid from '@mui/material/Grid'
import { WelcomeTile } from 'src/lib/components/home/WelcomeCard'

const Home = () => {
  return (
    <Grid container spacing={6}>
      <Grid item xs={8}>
        <WelcomeTile></WelcomeTile>
      </Grid>
      <Grid item xs={4}>
        <Health />
      </Grid>
      <Grid item xs={5}>
        <Pipelines />
      </Grid>
    </Grid>
  )
}

export default Home
