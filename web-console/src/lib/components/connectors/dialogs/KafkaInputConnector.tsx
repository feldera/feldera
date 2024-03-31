// A create/update dialog for a Kafka input connector.
'use client'

import { GenericEditorForm } from '$lib/components/connectors/dialogs/tabs/GenericConnectorForm'
import { TabKafkaAuth } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaAuth'
import { TabKafkaInputDetails } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaInputDetails'
import { TabKafkaNameAndDesc } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaNameAndDesc'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import { TabGenericInputFormatDetails } from '$lib/components/connectors/dialogs/tabs/TabGenericInputFormatDetails'
import { TabLabel } from '$lib/components/connectors/dialogs/tabs/TabLabel'
import Transition from '$lib/components/connectors/dialogs/tabs/Transition'
import { connectorTransportName, parseKafkaInputSchema, parseKafkaInputSchemaConfig } from '$lib/functions/connectors'
import { authFields, authParamsSchema, defaultLibrdkafkaAuthOptions } from '$lib/functions/kafka/authParamsSchema'
import { LibrdkafkaOptionType, toKafkaConfig } from '$lib/functions/kafka/librdkafkaOptions'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { ConnectorType, Direction } from '$lib/types/connectors'
import { ConnectorDialogProps } from '$lib/types/connectors/ConnectorDialogProps'
import { useEffect, useState } from 'react'
import { FieldErrors } from 'react-hook-form'
import { FormContainer } from 'react-hook-form-mui'
import * as va from 'valibot'
import IconCategoryAlt from '~icons/bx/category-alt'
import IconData from '~icons/bx/data'
import IconFile from '~icons/bx/file'
import IconLockOpen from '~icons/bx/lock-open'
import IconX from '~icons/bx/x'

import { valibotResolver } from '@hookform/resolvers/valibot'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { DialogTitle, FormControlLabel, Switch, Tooltip } from '@mui/material'
import Box from '@mui/material/Box'
import Dialog from '@mui/material/Dialog'
import IconButton from '@mui/material/IconButton'
import Tab from '@mui/material/Tab'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify connector name')])),
  description: va.optional(va.string(), ''),
  transport: va.intersect([
    va.object(
      {
        bootstrap_servers: va.nonOptional(
          va.array(va.string([va.minLength(1, 'Specify at least one server')]), [
            va.minLength(1, 'Specify at least one server')
          ])
        ),
        auto_offset_reset: va.optional(
          va.picklist(['smallest', 'earliest', 'beginning', 'largest', 'latest', 'end', 'error', 'Invalid enum value']),
          'earliest'
        ),
        group_id: va.optional(va.string([va.minLength(1, 'group.id should not be empty')])),
        topics: va.nonOptional(
          va.array(va.string([va.minLength(1, 'Topic name should not be empty')]), [
            va.minLength(1, 'Provide at least one topic')
          ])
        ),
        preset_service: va.optional(va.string([va.toCustom(s => (s === '' ? undefined! : s))]))
      },
      // Allow configurations options not mentioned in the schema
      va.union([va.string(), va.number(), va.boolean(), va.array(va.string()), va.any()])
    ),
    authParamsSchema
  ]),
  format: va.object({
    format_name: va.nonOptional(va.picklist(['json', 'csv'])),
    update_format: va.optional(va.picklist(['raw', 'insert_delete']), 'raw'),
    json_array: va.nonOptional(va.boolean())
  })
})
export type KafkaInputSchema = va.Input<typeof schema>

export const KafkaInputConnectorDialog = (props: ConnectorDialogProps) => {
  const tabs = ['detailsTab', 'sourceTab', 'authTab', 'formatTab'] as const
  const [rawJSON, setRawJSON] = useState(false)
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('detailsTab')
  const [curValues, setCurValues] = useState<KafkaInputSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseKafkaInputSchema(props.connector))
    }
  }, [props.connector])

  const defaultValues: KafkaInputSchema = {
    name: '',
    description: '',
    transport: {
      bootstrap_servers: [''],
      auto_offset_reset: 'earliest',
      topics: [],
      ...defaultLibrdkafkaAuthOptions
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

  const normalizeConfig = (data: {
    transport: Record<string, LibrdkafkaOptionType>
    format: Record<string, string | boolean>
  }) => ({
    transport: {
      name: connectorTransportName(ConnectorType.KAFKA_IN),
      config: toKafkaConfig(data.transport)
    },
    format: {
      name: data.format.format_name,
      config: {
        ...(data.format.format_name === 'json'
          ? {
              update_format: data.format.update_format,
              array: data.format.json_array
            }
          : {})
      }
    }
  })

  // Define what should happen when the form is submitted
  const prepareData = (data: KafkaInputSchema) => ({
    name: data.name,
    description: data.description,
    config: normalizeConfig(data)
  })

  const onSubmit = useConnectorRequest(props.connector, prepareData, props.onSuccess, handleClose)

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = ({ name, description, transport, format }: FieldErrors<KafkaInputSchema>) => {
    if (!props.show) {
      return
    }
    if (name || description) {
      setActiveTab('detailsTab')
    } else if (
      transport?.bootstrap_servers ||
      transport?.topics ||
      transport?.['auto_offset_reset'] ||
      transport?.['group_id']
    ) {
      setActiveTab('sourceTab')
    } else if (transport && authFields.some(f => f in transport)) {
      setActiveTab('authTab')
    } else if (format?.format_name || format?.json_array || format?.update_format) {
      setActiveTab('formatTab')
    }
  }

  const tabFooter = <TabFooter submitButton={props.submitButton} {...{ activeTab, setActiveTab, tabs }} />
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
          {props.connector === undefined ? 'New Kafka Datasource' : props.existingTitle?.(props.connector.name) ?? ''}
        </DialogTitle>
        <IconButton
          onClick={handleClose}
          sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          data-testid='button-close-modal'
        >
          <IconX />
        </IconButton>
        {jsonSwitch}
        <Box sx={{ height: '70vh' }}>
          {rawJSON ? (
            <Box sx={{ height: '100%' }}>
              <Box sx={{ display: 'flex', flexDirection: 'column', p: 4, height: '100%' }}>
                <GenericEditorForm
                  disabled={props.disabled}
                  direction={Direction.INPUT}
                  configFromText={text => parseKafkaInputSchemaConfig(JSON.parse(text))}
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
                          icon={<IconFile />}
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
                          subtitle='Source details'
                          active={activeTab === 'sourceTab'}
                          icon={<IconData />}
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
                          icon={<IconLockOpen />}
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
                          icon={<IconCategoryAlt />}
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
                        direction={Direction.INPUT}
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
                      <TabKafkaInputDetails disabled={props.disabled} parentName='transport' />
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
                      <TabGenericInputFormatDetails disabled={props.disabled} />
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
