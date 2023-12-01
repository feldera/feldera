// Form to edit the name and description of a config.

import { useBuilderState } from '$lib/compositions/streaming/builder/useBuilderState'
import useDebouncedSave from '$lib/compositions/streaming/builder/useDebouncedSave'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'

import { FormControl, FormHelperText, TextField } from '@mui/material'
import Grid from '@mui/material/Grid'

interface FormError {
  name?: { message?: string }
}

const Metadata = (props: { errors: FormError }) => {
  const savePipeline = useDebouncedSave()
  const setName = useBuilderState(state => state.setName)
  const setDescription = useBuilderState(state => state.setDescription)
  const name = useBuilderState(state => state.name)
  const description = useBuilderState(state => state.description)

  const updateName = (event: React.ChangeEvent<HTMLInputElement>) => {
    setName(event.target.value)
    savePipeline()
  }

  const updateDescription = (event: React.ChangeEvent<HTMLInputElement>) => {
    setDescription(event.target.value)
    savePipeline()
  }

  return (
    <Grid container spacing={5}>
      <Grid item xs={4}>
        <FormControl fullWidth>
          <TextField
            fullWidth
            id='pipeline-name' // referenced by webui-tester
            type='text'
            label='Name'
            placeholder={PLACEHOLDER_VALUES['pipeline_name']}
            value={name}
            error={Boolean(props.errors.name)}
            onChange={updateName}
            inputProps={{ 'data-testid': 'input-pipeline-name' }}
          />
          {props.errors.name && (
            <FormHelperText sx={{ color: 'error.main' }} id='validation-schema-first-name'>
              {props.errors.name.message}
            </FormHelperText>
          )}
        </FormControl>
      </Grid>
      <Grid item xs={8}>
        <TextField
          fullWidth
          id='pipeline-description' // referenced by webui-tester
          type='Description'
          label='Description'
          placeholder={PLACEHOLDER_VALUES['pipeline_description']}
          value={description}
          onChange={updateDescription}
          inputProps={{ 'data-testid': 'input-pipeline-description' }}
        />
      </Grid>
    </Grid>
  )
}

export default Metadata
