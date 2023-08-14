import {
  AddGenericConnectorCard,
  AddKafkaInputConnectorCard,
  AddKafkaOutputConnectorCard,
  AddUrlConnectorCard
} from '$lib/components/connectors/dialogs'

import Grid from '@mui/material/Grid'
import { usePageHeader } from 'src/lib/compositions/global/pageHeader'

const ConnectorCreateGrid = () => {
  usePageHeader(s => s.setHeader)({ title: null })
  // id is referenced by webui-tester
  return (
    <Grid id='connector-creator-content' container spacing={6} className='match-height'>
      <Grid item md={4} sm={6} xs={12}>
        <AddUrlConnectorCard />
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

export default ConnectorCreateGrid
