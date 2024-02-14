import { GridItems } from '$lib/components/common/GridItems'
import { KafkaAuthElement } from '$lib/components/connectors/dialogs/elements/kafka/AuthElement'

import { Grid } from '@mui/material'

export const TabKafkaAuth = (props: { disabled?: boolean; parentName: string }) => {
  return (
    <Grid container spacing={4} sx={{ overflowY: 'auto' }}>
      <GridItems xs={12}>
        <KafkaAuthElement {...props}></KafkaAuthElement>
      </GridItems>
    </Grid>
  )
}
