'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import {
  AddGenericConnectorCard,
  AddKafkaInputConnectorCard,
  AddKafkaOutputConnectorCard,
  AddUrlConnectorCard
} from '$lib/components/connectors/dialogs'

import { Link } from '@mui/material'
import Grid from '@mui/material/Grid'

const ConnectorCreateGrid = () => {
  // id is referenced by webui-tester
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/connectors/list`}>Connectors</Link>
        <Link href={`/connectors/create`}>Create</Link>
      </BreadcrumbsHeader>
      <Grid id='connector-creator-content' container spacing={6} className='match-height' sx={{ pl: 6, pt: 6 }}>
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
    </>
  )
}

export default ConnectorCreateGrid
