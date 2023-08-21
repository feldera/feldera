// A create/update dialog for a Kafka input connector.

import TabFooter from '$lib/components/connectors/dialogs/tabs/TabFooter'
import TabLabel from '$lib/components/connectors/dialogs/tabs/TabLabel'
import { ConnectorFormNewRequest, ConnectorFormUpdateRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { connectorTypeToConfig, connectorTypeToIcon, parseUrlSchema } from '$lib/functions/connectors'
import { ConnectorType } from '$lib/types/connectors'
import ConnectorDialogProps from '$lib/types/connectors/ConnectorDialogProps'
import {
  ConnectorDescr,
  ConnectorId,
  FormatConfig,
  NewConnectorRequest,
  UpdateConnectorRequest
} from '$lib/services/manager'
import { useEffect, useState } from 'react'
import { Controller, useForm } from 'react-hook-form'
import * as yup from 'yup'

import { yupResolver } from '@hookform/resolvers/yup'
import { Icon } from '@iconify/react'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import Box from '@mui/material/Box'
import Dialog from '@mui/material/Dialog'
import DialogContent from '@mui/material/DialogContent'
import IconButton from '@mui/material/IconButton'
import Tab from '@mui/material/Tab'
import Typography from '@mui/material/Typography'

import { AddConnectorCard } from './AddConnectorCard'
import Transition from './tabs/Transition'
import TabInputFormatDetails from './tabs/TabInputFormatDetails'
import { FormControl, FormHelperText, Grid, TextField } from '@mui/material'
import { PLACEHOLDER_VALUES } from 'src/lib/functions/placeholders'

const schema = yup.object().shape({
  name: yup.string().required(),
  description: yup.string().default(''),
  url: yup.string().required(),
  format_name: yup.string().required().oneOf(['json', 'csv']),
  json_update_format: yup.string().oneOf(['raw', 'insert_delete']).default('raw'),
  json_array: yup.bool().required()
})

export type UrlSchema = yup.InferType<typeof schema>

export const UrlConnectorDialog = (props: ConnectorDialogProps) => {
  const [activeTab, setActiveTab] = useState<string>('detailsTab')
  const [curValues, setCurValues] = useState<UrlSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseUrlSchema(props.connector))
    }
  }, [props.connector])

  const {
    control,
    reset,
    watch,
    handleSubmit,
    formState: { errors }
  } = useForm<UrlSchema>({
    resolver: yupResolver(schema),
    defaultValues: {
      name: '',
      description: '',
      url: '',
      format_name: 'json',
      json_update_format: 'raw',
      json_array: false
    },
    values: curValues
  })

  const handleClose = () => {
    reset()
    setActiveTab('detailsTab')
    props.setShow(false)
  }

  const onFormSubmitted = (connector: ConnectorDescr | undefined) => {
    handleClose()
    if (connector !== undefined && props.onSuccess !== undefined) {
      props.onSuccess(connector)
    }
  }

  // Define what should happen when the form is submitted
  const genericRequest = (
    data: UrlSchema,
    connector_id?: string
  ): [ConnectorId | undefined, NewConnectorRequest | UpdateConnectorRequest] => {
    const format: FormatConfig = {
      name: data.format_name,
      config:
        data.format_name === 'json'
          ? {
              update_format: data.json_update_format,
              array: data.json_array
            }
          : {}
    }

    return [
      connector_id,
      {
        name: data.name,
        description: data.description,
        config: {
          transport: {
            name: connectorTypeToConfig(ConnectorType.URL),
            config: {
              path: data.url
            }
          },
          format: format
        }
      }
    ]
  }

  const newRequest = (data: UrlSchema): [undefined, NewConnectorRequest] => {
    return genericRequest(data) as [undefined, NewConnectorRequest]
  }
  const updateRequest = (data: UrlSchema): [ConnectorId, UpdateConnectorRequest] => {
    return genericRequest(data, props.connector?.connector_id) as [ConnectorId, UpdateConnectorRequest]
  }

  const onSubmit =
    props.connector === undefined
      ? ConnectorFormNewRequest<UrlSchema>(onFormSubmitted, newRequest)
      : ConnectorFormUpdateRequest<UrlSchema>(onFormSubmitted, updateRequest)

  // If there is an error, switch to the earliest tab with an error
  useEffect(() => {
    if ((errors?.name || errors?.description || errors?.url) && props.show) {
      setActiveTab('detailsTab')
    } else if ((errors?.format_name || errors?.json_array || errors?.json_update_format) && props.show) {
      setActiveTab('formatTab')
    }
  }, [props.show, errors])

  const tabList = ['detailsTab', 'formatTab']
  return (
    <Dialog
      fullWidth
      open={props.show}
      scroll='body'
      maxWidth='md'
      onClose={handleClose}
      TransitionComponent={Transition}
    >
      <form id='create-url-resource' onSubmit={handleSubmit(onSubmit)}>
        <DialogContent
          sx={{
            pt: { xs: 8, sm: 12.5 },
            pr: { xs: 5, sm: 12 },
            pb: { xs: 5, sm: 9.5 },
            pl: { xs: 4, sm: 11 },
            position: 'relative'
          }}
        >
          <IconButton size='small' onClick={handleClose} sx={{ position: 'absolute', right: '1rem', top: '1rem' }}>
            <Icon icon='bx:x' />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {props.connector === undefined ? 'New URL' : 'Update ' + props.connector.name}
            </Typography>
            {props.connector === undefined && <Typography variant='body2'>Provide the URL to a data source</Typography>}
          </Box>
          <Box sx={{ display: 'flex', flexWrap: { xs: 'wrap', md: 'nowrap' } }}>
            <TabContext value={activeTab}>
              <TabList
                orientation='vertical'
                onChange={(e, newValue: string) => setActiveTab(newValue)}
                sx={{
                  border: 0,
                  minWidth: 200,
                  '& .MuiTabs-indicator': { display: 'none' },
                  '& .MuiTabs-flexContainer': {
                    alignItems: 'flex-start',
                    '& .MuiTab-root': {
                      width: '100%',
                      alignItems: 'flex-start'
                    }
                  }
                }}
              >
                <Tab
                  disableRipple
                  value='detailsTab'
                  label={
                    <TabLabel
                      title='Source'
                      subtitle='Description'
                      active={activeTab === 'detailsTab'}
                      icon={<Icon icon='bx:file' />}
                    />
                  }
                />
                <Tab
                  disableRipple
                  value='formatTab'
                  label={
                    <TabLabel
                      title='Format'
                      active={activeTab === 'formatTab'}
                      subtitle='Data details'
                      icon={<Icon icon='lucide:file-json-2' />}
                    />
                  }
                />
              </TabList>
              <TabPanel
                value='detailsTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                <Grid container spacing={6}>
                  <Grid item sm={4} xs={12}>
                    <FormControl fullWidth>
                      <Controller
                        name='name'
                        control={control}
                        render={({ field }) => (
                          <TextField
                            label='Datasource Name'
                            placeholder={PLACEHOLDER_VALUES['connector_name']}
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
                            placeholder={PLACEHOLDER_VALUES['connector_description']}
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
                            label='URL'
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
                </Grid>

                <TabFooter
                  isUpdate={props.connector !== undefined}
                  activeTab={activeTab}
                  setActiveTab={setActiveTab}
                  formId='create-url-resource'
                  tabsArr={tabList}
                />
              </TabPanel>
              <TabPanel
                value='formatTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                {/* @ts-ignore: TODO: This type mismatch seems like a bug in hook-form and/or resolvers */}
                <TabInputFormatDetails control={control} errors={errors} watch={watch} />
                <TabFooter
                  isUpdate={props.connector !== undefined}
                  activeTab={activeTab}
                  setActiveTab={setActiveTab}
                  formId='create-url-resource'
                  tabsArr={tabList}
                />
              </TabPanel>
            </TabContext>
          </Box>
        </DialogContent>
      </form>
    </Dialog>
  )
}

export const AddUrlConnectorCard = () => {
  return (
    <AddConnectorCard
      icon={connectorTypeToIcon(ConnectorType.URL)}
      title='Load Data from an HTTP URL.'
      dialog={UrlConnectorDialog}
    />
  )
}
