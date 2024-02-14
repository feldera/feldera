import { GridItems } from '$lib/components/common/GridItems'
import { KafkaBootstrapServersElement } from '$lib/components/connectors/dialogs/elements/kafka/Common'
import { KafkaTopicElement } from '$lib/components/connectors/dialogs/elements/kafka/Egress'

import Grid from '@mui/material/Grid'

export const TabKafkaOutputDetails = (props: { disabled?: boolean; parentName: string }) => {
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <KafkaBootstrapServersElement {...props}></KafkaBootstrapServersElement>

        <KafkaTopicElement {...props}></KafkaTopicElement>
      </GridItems>
    </Grid>
  )
}
