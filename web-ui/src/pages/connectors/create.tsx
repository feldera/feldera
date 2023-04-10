import Grid from '@mui/material/Grid'

import { AddCsvFileConnectorCard } from 'src/connectors/dialogs/CsvFileConnector'
import { AddKafkaOutputConnectorCard } from 'src/connectors/dialogs/KafkaOutputConnector'
import { AddKafkaInputConnectorCard } from 'src/connectors/dialogs/KafkaInputConnector'

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
    </Grid>
  )
}

export default DialogExamples
