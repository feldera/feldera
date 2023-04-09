// ** React Imports
import { Dispatch, SetStateAction, useState } from 'react'

// ** MUI Imports
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Dialog from '@mui/material/Dialog'
import Button from '@mui/material/Button'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'
import CardContent from '@mui/material/CardContent'
import DialogContent from '@mui/material/DialogContent'

import { Icon } from '@iconify/react'
import YAML from 'yaml'
import * as yup from 'yup'
import { yupResolver } from '@hookform/resolvers/yup'
import { Controller, useForm } from 'react-hook-form'

import Transition from './create-app-tabs/Transition'
import { ConnectorType, ConnectorDescr } from 'src/types/manager'
import { SourceFormCreateHandle } from './SubmitHandler'
import Grid from '@mui/material/Grid'
import FormControl from '@mui/material/FormControl'
import TextField from '@mui/material/TextField'
import FormControlLabel from '@mui/material/FormControlLabel'
import Switch from '@mui/material/Switch'
import DialogActions from '@mui/material/DialogActions'
import FormHelperText from '@mui/material/FormHelperText'
import { connectorTypeToConfig } from 'src/types/data'

const schema = yup
  .object({
    name: yup.string().required(),
    description: yup.string().default(''),
    url: yup.string().required(),
    has_headers: yup.boolean().default(true)
  })
  .required()

type CsvFileSource = yup.InferType<typeof schema>

export const DialogCreateCsv = (props: {
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  onSuccess?: Dispatch<ConnectorDescr>
}) => {
  const handleClose = () => {
    props.setShow(false)
  }

  const onFormSubmitted = (descr: ConnectorDescr | undefined) => {
    handleClose()
    if (descr !== undefined && props.onSuccess !== undefined) {
      props.onSuccess(descr)
    }
  }

  const {
    control,
    handleSubmit,
    formState: { errors }
  } = useForm<CsvFileSource>({
    resolver: yupResolver(schema),
    defaultValues: {
      name: '',
      description: '',
      url: '',
      has_headers: true
    }
  })

  // Add a new URL source
  const onSubmit = SourceFormCreateHandle<CsvFileSource>(onFormSubmitted, data => {
    return {
      name: data.name,
      description: data.description,
      typ: ConnectorType.FILE,
      config: YAML.stringify({
        transport: {
          name: connectorTypeToConfig(ConnectorType.FILE),
          config: {
            path: data.url
          }
        },
        format: {
          name: 'csv'
        }
      })
    }
  })

  return (
    <Dialog
      fullWidth
      open={props.show}
      maxWidth='md'
      scroll='body'
      onClose={() => props.setShow(false)}
      TransitionComponent={Transition}
    >
      <form id='create-csv-file' onSubmit={handleSubmit(onSubmit)}>
        <DialogContent sx={{ pb: 8, px: { xs: 8, sm: 15 }, pt: { xs: 8, sm: 12.5 }, position: 'relative' }}>
          <IconButton
            size='small'
            onClick={() => props.setShow(false)}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              CSV File
            </Typography>
            <Typography variant='body2'>Provide the URL to a CSV file</Typography>
          </Box>
          <Grid container spacing={6}>
            <Grid item sm={4} xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='name'
                  control={control}
                  render={({ field }) => (
                    <TextField
                      label='Datasource Name'
                      placeholder='AAPL'
                      error={Boolean(errors.name)}
                      aria-describedby='validation-name'
                      {...field}
                    />
                  )}
                />
                {errors.name && (
                  <FormHelperText sx={{ color: 'error.main' }} id='validation-name'>
                    {errors.name.message}
                  </FormHelperText>
                )}
              </FormControl>
            </Grid>
            <Grid item sm={8} xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='description'
                  control={control}
                  render={({ field }) => (
                    <TextField
                      fullWidth
                      label='Description'
                      placeholder='5 min Stock Ticker Data'
                      error={Boolean(errors.description)}
                      aria-describedby='validation-description'
                      {...field}
                    />
                  )}
                />
                {errors.description && (
                  <FormHelperText sx={{ color: 'error.main' }} id='validation-description'>
                    {errors.description.message}
                  </FormHelperText>
                )}
              </FormControl>
            </Grid>
            <Grid item sm={12} xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='url'
                  control={control}
                  render={({ field }) => (
                    <TextField
                      fullWidth
                      label='File Path'
                      placeholder='https://gist.githubusercontent.com/...'
                      error={Boolean(errors.description)}
                      aria-describedby='validation-description'
                      {...field}
                    />
                  )}
                />
                {errors.url && (
                  <FormHelperText sx={{ color: 'error.main' }} id='validation-description'>
                    {errors.url.message}
                  </FormHelperText>
                )}
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='has_headers'
                  control={control}
                  render={({ field }) => (
                    <FormControlLabel
                      control={<Switch checked={field.value} />}
                      label='CSV file has headers'
                      aria-describedby='header-description'
                      {...field}
                    />
                  )}
                />
              </FormControl>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions sx={{ pb: { xs: 8, sm: 12.5 }, justifyContent: 'center' }}>
          <Button
            variant='contained'
            sx={{ mr: 1 }}
            color='success'
            endIcon={<Icon icon='bx:check' />}
            form='create-csv-file'
            type='submit'
          >
            Create
          </Button>
          <Button variant='outlined' color='secondary' onClick={() => props.setShow(false)}>
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

const DialogCreateCsvBox = () => {
  const [show, setShow] = useState<boolean>(false)

  return (
    <Card>
      <CardContent sx={{ textAlign: 'center', '& svg': { mb: 2 } }}>
        <Icon icon='ph:file-csv' fontSize='4rem' />
        <Typography sx={{ mb: 3 }}>Provide data in the form of CSV files</Typography>
        <Button variant='contained' onClick={() => setShow(true)}>
          Add
        </Button>
      </CardContent>
      <DialogCreateCsv show={show} setShow={setShow} />
    </Card>
  )
}

export default DialogCreateCsvBox
