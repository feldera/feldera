import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { PipelineDescr, UpdatePipelineRequest } from '$lib/services/manager'
import { Dispatch, SetStateAction } from 'react'

import { FormControl, FormHelperText, TextField } from '@mui/material'
import Grid from '@mui/material/Grid'

interface FormError {
  name?: { message?: string }
}

/**
 * Form to edit the name and description of a config.
 * @param props
 * @returns
 */
const Metadata = (props: {
  errors: FormError
  pipeline: PipelineDescr
  updatePipeline: Dispatch<SetStateAction<UpdatePipelineRequest>>
  disabled?: boolean
}) => {
  const updateName = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.updatePipeline(p => ({ ...p, name: event.target.value }))
  }

  const updateDescription = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.updatePipeline(p => ({ ...p, description: event.target.value }))
  }

  return (
    <Grid container spacing={5}>
      <Grid item xs={4}>
        <FormControl fullWidth>
          <TextField
            fullWidth
            type='text'
            label='Name'
            placeholder={PLACEHOLDER_VALUES['pipeline_name']}
            value={props.pipeline.name}
            error={Boolean(props.errors.name)}
            onChange={updateName}
            inputProps={{ 'data-testid': 'input-pipeline-name' }}
            disabled={props.disabled}
          />
          {props.errors.name && (
            <FormHelperText sx={{ color: 'error.main' }}>{props.errors.name.message}</FormHelperText>
          )}
        </FormControl>
      </Grid>
      <Grid item xs={8}>
        <TextField
          fullWidth
          type='Description'
          label='Description'
          placeholder={PLACEHOLDER_VALUES['pipeline_description']}
          value={props.pipeline.description}
          onChange={updateDescription}
          inputProps={{ 'data-testid': 'input-pipeline-description' }}
          disabled={props.disabled}
        />
      </Grid>
    </Grid>
  )
}

export default Metadata
