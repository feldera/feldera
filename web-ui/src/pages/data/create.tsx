import Grid from '@mui/material/Grid'

import DialogCreateCsv from 'src/data/DialogCreateCsv'
import DialogCreateKafkaOutBox from 'src/data/DialogCreateKafkaOut'
import DialogCreatePanda from 'src/data/DialogCreateRedpanda'

const DialogExamples = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <Grid item md={4} sm={6} xs={12}>
        <DialogCreateCsv />
      </Grid>
      <Grid item md={4} sm={6} xs={12}>
        <DialogCreatePanda />
      </Grid>
      <Grid item md={4} sm={6} xs={12}>
        <DialogCreateKafkaOutBox />
      </Grid>
    </Grid>
  )
}

export default DialogExamples
