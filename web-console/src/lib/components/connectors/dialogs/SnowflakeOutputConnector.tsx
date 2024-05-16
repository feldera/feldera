// A create/update dialog for a Kafka input connector.
'use client'

import { TabKafkaAuth } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaAuth'
import { TabKafkaNameAndDesc } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaNameAndDesc'
import { TabKafkaOutputDetails } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaOutputDetails'
import { SnowflakeOutputFormatDetails } from '$lib/components/connectors/dialogs/tabs/snowflake/SnowflakeOutputFormatDetails'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import { TabLabel } from '$lib/components/connectors/dialogs/tabs/TabLabel'
import Transition from '$lib/components/connectors/dialogs/tabs/Transition'
import {
  connectorTransportName,
  parseSnowflakeOutputSchema,
  parseSnowflakeOutputSchemaConfig
} from '$lib/functions/connectors'
import { authFields, authParamsSchema, defaultLibrdkafkaAuthOptions } from '$lib/functions/kafka/authParamsSchema'
import { LibrdkafkaOptionType, toKafkaConfig } from '$lib/functions/kafka/librdkafkaOptions'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { ConnectorType, Direction } from '$lib/types/connectors'
import { ConnectorDialogProps } from '$lib/types/connectors/ConnectorDialogProps'
import { useEffect, useState } from 'react'
import { FieldErrors } from 'react-hook-form'
import { FormContainer } from 'react-hook-form-mui'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { DialogTitle, FormControlLabel, Switch, Tooltip } from '@mui/material'
import Box from '@mui/material/Box'
import Dialog from '@mui/material/Dialog'
import IconButton from '@mui/material/IconButton'
import Tab from '@mui/material/Tab'

import { GenericEditorForm } from './tabs/GenericConnectorForm'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify connector name')])),
  description: va.optional(va.string(), ''),
  transport: va.intersect([
    va.object(
      {
        bootstrap_servers: va.optional(
          va.array(va.string([va.minLength(1, 'Specify at least one server')]), [
            va.minLength(1, 'Specify at least one server')
          ])
        ),
        topic: va.nonOptional(va.string([va.minLength(1, 'Topic name should not be empty')])),
        preset_service: va.optional(va.string([va.toCustom(s => (s === '' ? undefined! : s))]))
      },
      // Allow configurations options not mentioned in the schema
      va.union([va.string(), va.number(), va.boolean(), va.array(va.string()), va.any()])
    ),
    authParamsSchema
  ]),
  format: va.object({
    format_name: va.nonOptional(va.picklist(['json', 'avro'])),
    update_format: va.literal('snowflake')
  })
})
export type SnowflakeOutputSchema = va.Input<typeof schema>

export const SnowflakeOutputConnectorDialog = (props: ConnectorDialogProps) => {
  const tabs = ['detailsTab', 'sourceTab', 'authTab', 'formatTab'] as const
  const [rawJSON, setRawJSON] = useState(false)
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('detailsTab')
  const [curValues, setCurValues] = useState<SnowflakeOutputSchema | undefined>(undefined)

  // Initialize the form either with values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseSnowflakeOutputSchema(props.connector))
    }
  }, [props.connector])

  const defaultValues: SnowflakeOutputSchema = {
    name: '',
    description: '',
    transport: {
      bootstrap_servers: [''],
      topic: '',
      ...defaultLibrdkafkaAuthOptions
    },
    format: {
      format_name: 'json',
      update_format: 'snowflake'
    }
  }

  const handleClose = () => {
    setActiveTab(tabs[0])
    props.setShow(false)
  }

  // Define what should happen when the form is submitted
  const prepareData = (data: SnowflakeOutputSchema) => ({
    name: data.name,
    description: data.description,
    config: normalizeConfig(data)
  })

  const normalizeConfig = (data: {
    transport: Record<string, LibrdkafkaOptionType>
    format: Record<string, string | boolean>
  }) => ({
    transport: {
      name: connectorTransportName(ConnectorType.SNOWFLAKE_OUT),
      config: toKafkaConfig(data.transport)
    },
    format: {
      name: data.format.format_name,
      config: {
        update_format: data.format.update_format
      }
    }
  })

  const onSubmit = useConnectorRequest(props.connector, prepareData, props.onSuccess, handleClose)

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = ({ name, description, transport, format }: FieldErrors<SnowflakeOutputSchema>) => {
    if (!props.show) {
      return
    }
    if (name || description) {
      setActiveTab('detailsTab')
    } else if (transport?.bootstrap_servers || transport?.topic) {
      setActiveTab('sourceTab')
    } else if (transport && authFields.some(f => f in transport)) {
      setActiveTab('authTab')
    } else if (format?.format_name) {
      setActiveTab('formatTab')
    }
  }

  const tabFooter = (
    <TabFooter submitButton={props.submitButton} activeTab={activeTab} setActiveTab={setActiveTab} tabs={tabs} />
  )
  const [editorDirty, setEditorDirty] = useState<'dirty' | 'clean' | 'error'>('clean')

  const jsonSwitch = (
    <Box sx={{ pl: 4 }}>
      <Tooltip title={editorDirty !== 'clean' ? 'Fix errors before switching the view' : undefined}>
        <FormControlLabel
          control={<Switch checked={rawJSON} onChange={(e, v) => setRawJSON(v)} disabled={editorDirty !== 'clean'} />}
          label='Edit JSON'
        />
      </Tooltip>
    </Box>
  )
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
        values={curValues}
        defaultValues={defaultValues}
        onSuccess={onSubmit}
        onError={handleErrors}
      >
        <DialogTitle sx={{ textAlign: 'center' }}>
          {props.connector === undefined
            ? 'New Snowflake-Kafka Output'
            : props.existingTitle?.(props.connector.name) ?? ''}
        </DialogTitle>
        <IconButton
          onClick={handleClose}
          sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          data-testid='button-close-modal'
        >
          <i className={`bx bx-x`} style={{}} />
        </IconButton>
        {jsonSwitch}
        <Box sx={{ height: '70vh' }}>
          {rawJSON ? (
            <Box sx={{ height: '100%' }}>
              <Box sx={{ display: 'flex', flexDirection: 'column', p: 4, height: '100%' }}>
                <GenericEditorForm
                  disabled={props.disabled}
                  direction={Direction.OUTPUT}
                  configFromText={text => parseSnowflakeOutputSchemaConfig(JSON.parse(text))}
                  configToText={config => JSON.stringify(normalizeConfig(config), undefined, '\t')}
                  setEditorDirty={setEditorDirty}
                />
                <Box sx={{ display: 'flex', justifyContent: 'end', pt: 4 }}>{props.submitButton}</Box>
              </Box>
            </Box>
          ) : (
            <TabContext value={activeTab}>
              <Box sx={{ display: 'flex', flexWrap: { xs: 'wrap', md: 'nowrap' }, height: '100%' }}>
                <Box>
                  <TabList
                    orientation='vertical'
                    onChange={(e, newValue: (typeof tabs)[number]) => setActiveTab(newValue)}
                    sx={{
                      border: 0,
                      m: 0,
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
                          title='Metadata'
                          subtitle='Description'
                          active={activeTab === 'detailsTab'}
                          icon={<i className={`bx bx-file`} style={{}} />}
                        />
                      }
                      data-testid='button-tab-name'
                    />
                    <Tab
                      disableRipple
                      value='sourceTab'
                      label={
                        <TabLabel
                          title='Server'
                          subtitle='Sink details'
                          active={activeTab === 'sourceTab'}
                          icon={<i className={`bx bx-data`} style={{}} />}
                        />
                      }
                      data-testid='button-tab-server'
                    />
                    <Tab
                      disableRipple
                      value='authTab'
                      label={
                        <TabLabel
                          title='Security'
                          subtitle='Authentication protocol'
                          active={activeTab === 'authTab'}
                          icon={<i className={`bx bx-lock-open`} style={{}} />}
                        />
                      }
                      data-testid='button-tab-auth'
                    />
                    <Tab
                      disableRipple
                      value='formatTab'
                      label={
                        <TabLabel
                          title='Format'
                          subtitle='Data details'
                          active={activeTab === 'formatTab'}
                          icon={<i className={`bx bx-category-alt`} style={{}} />}
                        />
                      }
                      data-testid='button-tab-format'
                    />
                  </TabList>
                </Box>
                <Box sx={{ width: '100%' }}>
                  <TabPanel
                    value='detailsTab'
                    sx={{ border: 0, boxShadow: 0, p: 4, height: '100%', alignItems: 'start' }}
                  >
                    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                      <TabKafkaNameAndDesc
                        direction={Direction.OUTPUT}
                        disabled={props.disabled}
                        parentName='transport'
                      />
                      {tabFooter}
                    </Box>
                  </TabPanel>
                  <TabPanel
                    value='sourceTab'
                    sx={{ border: 0, boxShadow: 0, p: 4, height: '100%', alignItems: 'start' }}
                  >
                    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                      <TabKafkaOutputDetails disabled={props.disabled} parentName='transport' />
                      {tabFooter}
                    </Box>
                  </TabPanel>
                  <TabPanel value='authTab' sx={{ border: 0, boxShadow: 0, p: 4, height: '100%', alignItems: 'start' }}>
                    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                      <TabKafkaAuth disabled={props.disabled} parentName={'transport'} />
                      {tabFooter}
                    </Box>
                  </TabPanel>
                  <TabPanel
                    value='formatTab'
                    sx={{ border: 0, boxShadow: 0, p: 4, height: '100%', alignItems: 'start' }}
                  >
                    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                      <SnowflakeOutputFormatDetails disabled={props.disabled} />
                      {tabFooter}
                    </Box>
                  </TabPanel>
                </Box>
              </Box>
            </TabContext>
          )}
        </Box>
      </FormContainer>
    </Dialog>
  )
}
