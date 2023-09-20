// A create/update dialog for a Kafka input connector.
'use client'

import TabFooter from '$lib/components/connectors/dialogs/common/TabFooter'
import TabLabel from '$lib/components/connectors/dialogs/common/TabLabel'
import TabKafkaInputDetails from '$lib/components/connectors/dialogs/kafka/TabKafkaInputDetails'
import TabKafkaNameAndDesc from '$lib/components/connectors/dialogs/kafka/TabKafkaNameAndDesc'
import { connectorTypeToConfig, parseKafkaInputSchema } from '$lib/functions/connectors'
import {
  authFields,
  authParamsSchema,
  defaultUiAuthParams,
  prepareAuthData
} from '$lib/functions/kafka/authParamsSchema'
import { intersection } from '$lib/functions/valibot'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { ConnectorType } from '$lib/types/connectors'
import ConnectorDialogProps from '$lib/types/connectors/ConnectorDialogProps'
import { useEffect, useState } from 'react'
import { FieldErrors } from 'react-hook-form'
import { FormContainer } from 'react-hook-form-mui'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
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

import TabInputFormatDetails from './common/TabInputFormatDetails'
import Transition from './common/Transition'
import { TabKafkaAuth } from './kafka/TabKafkaAuth'

const schema = intersection([
  va.object({
    name: va.nonOptional(va.string()),
    description: va.optional(va.string(), ''),
    bootstrap_servers: va.nonOptional(va.string()),
    auto_offset_reset: va.optional(va.string(), 'none'),
    group_id: va.nonOptional(va.string()),
    topics: va.nonOptional(
      va.array(va.string([va.minLength(1, 'Topic name should be >=1 character')]), [
        va.minLength(1, 'Provide at least one topic (press enter to add the topic).')
      ])
    ),
    format_name: va.nonOptional(va.enumType(['json', 'csv'])),
    json_update_format: va.optional(va.enumType(['raw', 'insert_delete']), 'raw'),
    json_array: va.nonOptional(va.boolean())
  }),
  authParamsSchema
])
export type KafkaInputSchema = va.Input<typeof schema>

export const KafkaInputConnectorDialog = (props: ConnectorDialogProps) => {
  const [activeTab, setActiveTab] = useState<string>('detailsTab')
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
    bootstrap_servers: '',
    auto_offset_reset: 'earliest',
    topics: [],
    format_name: 'json',
    json_update_format: 'raw',
    json_array: false,
    group_id: '',
    ...defaultUiAuthParams
  }

  const handleClose = () => {
    setActiveTab(tabList[0])
    props.setShow(false)
  }

  // Define what should happen when the form is submitted
  const prepareData = (data: KafkaInputSchema) => ({
    name: data.name,
    description: data.description,
    config: {
      transport: {
        name: connectorTypeToConfig(ConnectorType.KAFKA_IN),
        config: {
          'bootstrap.servers': data.bootstrap_servers,
          'auto.offset.reset': data.auto_offset_reset,
          'group.id': data.group_id,
          topics: data.topics,
          ...prepareAuthData(data)
        }
      },
      format: {
        name: data.format_name,
        config:
          data.format_name === 'json'
            ? {
                update_format: data.json_update_format,
                array: data.json_array
              }
            : {}
      }
    }
  })

  const onSubmit = useConnectorRequest(props.connector, prepareData, props.onSuccess, handleClose)

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = (errors: FieldErrors<KafkaInputSchema>) => {
    if (!props.show) {
      return
    }
    if (errors?.name || errors?.description) {
      setActiveTab('detailsTab')
    } else if (errors?.bootstrap_servers || errors?.topics || errors?.['auto_offset_reset'] || errors?.['group_id']) {
      setActiveTab('sourceTab')
    } else if (authFields.some(f => f in errors)) {
      setActiveTab('authTab')
    } else if (errors?.format_name || errors?.json_array || errors?.json_update_format) {
      setActiveTab('formatTab')
    }
  }

  const tabList = ['detailsTab', 'sourceTab', 'authTab', 'formatTab']
  const tabFooter = (
    <TabFooter
      isUpdate={props.connector !== undefined}
      activeTab={activeTab}
      setActiveTab={setActiveTab}
      tabsArr={tabList}
    />
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
              {props.connector === undefined ? 'New Kafka Datasource' : 'Update ' + props.connector.name}
            </Typography>
            {props.connector === undefined && <Typography variant='body2'>Add a Kafka Input.</Typography>}
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
                      title='Metadata'
                      subtitle='Description'
                      active={activeTab === 'detailsTab'}
                      icon={<Icon icon='bx:file' />}
                    />
                  }
                />
                <Tab
                  disableRipple
                  value='sourceTab'
                  label={
                    <TabLabel
                      title='Server'
                      subtitle='Source details'
                      active={activeTab === 'sourceTab'}
                      icon={<Icon icon='bx:data' />}
                    />
                  }
                />
                <Tab
                  disableRipple
                  value='authTab'
                  label={
                    <TabLabel
                      title='Security'
                      subtitle='Authentication protocol'
                      active={activeTab === 'authTab'}
                      icon={<Icon icon='bx:lock-open' />}
                    />
                  }
                />
                <Tab
                  disableRipple
                  value='formatTab'
                  label={
                    <TabLabel
                      title='Format'
                      subtitle='Data details'
                      active={activeTab === 'formatTab'}
                      icon={<Icon icon='lucide:file-json-2' />}
                    />
                  }
                />
              </TabList>
              <TabPanel
                value='detailsTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                <TabKafkaNameAndDesc />
                {tabFooter}
              </TabPanel>
              <TabPanel
                value='sourceTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                <TabKafkaInputDetails />
                {tabFooter}
              </TabPanel>
              <TabPanel value='authTab' sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}>
                <TabKafkaAuth />
                {tabFooter}
              </TabPanel>
              <TabPanel
                value='formatTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                <TabInputFormatDetails />
                {tabFooter}
              </TabPanel>
            </TabContext>
          </Box>
        </DialogContent>
      </FormContainer>
    </Dialog>
  )
}
