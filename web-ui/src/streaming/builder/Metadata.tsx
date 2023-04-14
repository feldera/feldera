// Form to edit the name and description of a config.

import { FormControl, FormHelperText, TextField } from '@mui/material'
import Grid from '@mui/material/Grid'
import useDebouncedSave from './hooks/useDebouncedSave'
import { useBuilderState } from './useBuilderState'

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
            type='text'
            label='Name'
            placeholder='NYSE Algotrading Pipeline'
            value={name}
            error={Boolean(props.errors.name)}
            onChange={updateName}
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
          type='Description'
          label='Description'
          placeholder='Uses Pair Trading Strategy'
          value={description}
          onChange={updateDescription}
        />
      </Grid>
    </Grid>
  )
}

export default Metadata
