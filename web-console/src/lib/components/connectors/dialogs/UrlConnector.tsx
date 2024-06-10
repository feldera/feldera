// A create/update dialog for a Kafka input connector.
'use client'

import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import { TabLabel } from '$lib/components/connectors/dialogs/tabs/TabLabel'
import { parseUrlSchema } from '$lib/functions/connectors'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { TransportConfig } from '$lib/services/manager'
import { ConnectorDialogProps } from '$lib/types/connectors/ConnectorDialogProps'
import { useEffect, useState } from 'react'
import { FieldErrors } from 'react-hook-form'
import { FormContainer, TextFieldElement } from 'react-hook-form-mui'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { Grid } from '@mui/material'
import Box from '@mui/material/Box'
import Dialog from '@mui/material/Dialog'
import DialogContent from '@mui/material/DialogContent'
import IconButton from '@mui/material/IconButton'
import Tab from '@mui/material/Tab'
import Typography from '@mui/material/Typography'

import { TabGenericInputFormatDetails } from './tabs/generic/TabGenericInputFormatDetails'
import Transition from './tabs/Transition'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify connector name')])),
  description: va.optional(va.string(), ''),
  transport: va.object({
    url: va.nonOptional(va.string())
  }),
  format: va.object({
    format_name: va.nonOptional(va.picklist(['json', 'csv'])),
    update_format: va.optional(va.picklist(['raw', 'insert_delete']), 'raw'),
    json_array: va.nonOptional(va.boolean())
  })
})

export type UrlSchema = va.Input<typeof schema>

export const UrlConnectorDialog = (props: ConnectorDialogProps) => {
  const tabs = ['detailsTab', 'formatTab'] as const
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('detailsTab')
  const [curValues, setCurValues] = useState<UrlSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseUrlSchema(props.connector))
    }
  }, [props.connector])

  const defaultValues: UrlSchema = {
    name: '',
    description: '',
    transport: {
      url: ''
    },
    format: {
      format_name: 'json',
      update_format: 'raw',
      json_array: false
    }
  }

  const handleClose = () => {
    setActiveTab(tabs[0])
    props.setShow(false)
  }

  // Define what should happen when the form is submitted
  const prepareData = (data: UrlSchema) => ({
    name: data.name,
    description: data.description,
    config: normalizeConfig(data)
  })

  const normalizeConfig = (data: { transport: UrlSchema['transport']; format: UrlSchema['format'] }) => ({
    transport: {
      name: TransportConfig.name.URL_INPUT,
      config: {
        path: data.transport.url
      }
    },
    format: {
      name: data.format.format_name,
      config:
        data.format.format_name === 'json'
          ? {
              update_format: data.format.update_format,
              array: data.format.json_array
            }
          : {}
    }
  })

  const onSubmit = useConnectorRequest(props.connector, prepareData, props.onSuccess, handleClose)

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = ({ name, description, transport, format }: FieldErrors<UrlSchema>) => {
    if (!props.show) {
      return
    }
    if (name || description || transport?.url) {
      setActiveTab('detailsTab')
    } else if (format?.format_name || format?.json_array || format?.update_format) {
      setActiveTab('formatTab')
    }
  }
  const tabFooter = <TabFooter submitButton={props.submitButton} {...{ activeTab, setActiveTab, tabs }} />
  return (
    <Dialog
      fullWidth
      open={props.show}
      scroll='body'
      maxWidth='md'
      onClose={handleClose}
      TransitionComponent={Transition}
    >
      <FormContainer
        resolver={valibotResolver(schema)}
        mode='onChange'
        values={curValues}
        defaultValues={defaultValues}
        onSuccess={onSubmit}
        onError={handleErrors}
      >
        <DialogContent
          sx={{
            pt: { xs: 8, sm: 12.5 },
            pr: { xs: 5, sm: 12 },
            pb: { xs: 5, sm: 9.5 },
            pl: { xs: 4, sm: 11 },
            position: 'relative'
          }}
        >
          <IconButton
            size='small'
            onClick={handleClose}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
            data-testid='button-close-modal'
          >
            <i className={`bx bx-x`} style={{}} />
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
                onChange={(e, newValue: (typeof tabs)[number]) => setActiveTab(newValue)}
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
                      icon={<i className={`bx bx-file`} style={{}} />}
                    />
                  }
                  data-testid='button-tab-name'
                />
                <Tab
                  disableRipple
                  value='formatTab'
                  label={
                    <TabLabel
                      title='Format'
                      active={activeTab === 'formatTab'}
                      subtitle='Data details'
                      icon={<i className={`bx bx-category-alt`} style={{}} />}
                    />
                  }
                  data-testid='button-tab-format'
                />
              </TabList>
              <TabPanel
                value='detailsTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                <Grid container spacing={4}>
                  <Grid item sm={4} xs={12}>
                    <TextFieldElement
                      name='name'
                      label='Data Source Name'
                      size='small'
                      fullWidth
                      placeholder={PLACEHOLDER_VALUES['connector_name']}
                      aria-describedby='validation-name'
                      disabled={props.disabled}
                      inputProps={{
                        'data-testid': 'input-datasource-name'
                      }}
                    />
                  </Grid>
                  <Grid item sm={8} xs={12}>
                    <TextFieldElement
                      name='description'
                      label='Description'
                      size='small'
                      fullWidth
                      placeholder={PLACEHOLDER_VALUES['connector_description']}
                      aria-describedby='validation-description'
                      disabled={props.disabled}
                      inputProps={{
                        'data-testid': 'input-datasource-description'
                      }}
                    />
                  </Grid>
                  <Grid item sm={12} xs={12}>
                    <TextFieldElement
                      name='transport.url'
                      label='URL'
                      size='small'
                      fullWidth
                      placeholder='https://gist.githubusercontent.com/...'
                      aria-describedby='validation-description'
                      disabled={props.disabled}
                      inputProps={{
                        'data-testid': 'input-datasource-url'
                      }}
                    />
                  </Grid>
                </Grid>

                {tabFooter}
              </TabPanel>
              <TabPanel
                value='formatTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                {/* @ts-ignore: TODO: This type mismatch seems like a bug in hook-form and/or resolvers */}
                <TabGenericInputFormatDetails disabled={props.disabled} />
                {tabFooter}
              </TabPanel>
            </TabContext>
          </Box>
        </DialogContent>
      </FormContainer>
    </Dialog>
  )
}
