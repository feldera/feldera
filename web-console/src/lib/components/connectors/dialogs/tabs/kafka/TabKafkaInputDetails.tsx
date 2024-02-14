import { GridItems } from '$lib/components/common/GridItems'
import { KafkaBootstrapServersElement } from '$lib/components/connectors/dialogs/elements/kafka/Common'
import {
  KafkaAutoOffsetResetElement,
  KafkaGroupIdElement,
  KafkaTopicsElement
} from '$lib/components/connectors/dialogs/elements/kafka/Ingress'

import Grid from '@mui/material/Grid'

export const TabKafkaInputDetails = (props: { disabled?: boolean; parentName: string }) => {
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <KafkaBootstrapServersElement {...props}></KafkaBootstrapServersElement>
        <KafkaAutoOffsetResetElement {...props}></KafkaAutoOffsetResetElement>
        <KafkaGroupIdElement {...props}></KafkaGroupIdElement>
        <KafkaTopicsElement {...props}></KafkaTopicsElement>
      </GridItems>
    </Grid>
  )
}
