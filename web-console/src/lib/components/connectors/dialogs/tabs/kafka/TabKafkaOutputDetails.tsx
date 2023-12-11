import { GridItems } from '$lib/components/common/GridItems'
import { TextFieldElement } from 'react-hook-form-mui'

import Grid from '@mui/material/Grid'

const TabkafkaOutputDetails = (props: { disabled?: boolean }) => {
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <TextFieldElement
          name='bootstrap_servers'
          label='bootstrap.servers'
          size='small'
          helperText='Bootstrap Server Hostname'
          fullWidth
          placeholder='kafka.example.com'
          aria-describedby='validation-host'
          disabled={props.disabled}
        />

        <TextFieldElement
          name='topic'
          label='Topic Name'
          size='small'
          fullWidth
          placeholder='my-topic'
          aria-describedby='validation-topic'
          disabled={props.disabled}
        />
      </GridItems>
    </Grid>
  )
}

export default TabkafkaOutputDetails
