import Grid from '@mui/material/Grid'
import {
  AddGenericConnectorCard,
  AddCsvFileConnectorCard,
  AddKafkaOutputConnectorCard,
  AddKafkaInputConnectorCard
} from 'src/connectors/dialogs'

const DialogExamples = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <Grid item md={4} sm={6} xs={12}>
        <AddCsvFileConnectorCard />
      </Grid>
      <Grid item md={4} sm={6} xs={12}>
        <AddKafkaInputConnectorCard />
      </Grid>
      <Grid item md={4} sm={6} xs={12}>
        <AddKafkaOutputConnectorCard />
      </Grid>
      <Grid item md={4} sm={6} xs={12}>
        <AddGenericConnectorCard />
      </Grid>
    </Grid>
  )
}

export default DialogExamples
