// A create/update dialog for a Kafka output connector.
'use client'

import TabKafkaNameAndDesc from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaNameAndDesc'
import TabFooter from '$lib/components/connectors/dialogs/tabs/TabFooter'
import TabLabel from '$lib/components/connectors/dialogs/tabs/TabLabel'
import { connectorTransportName, parseKafkaOutputSchema } from '$lib/functions/connectors'
import {
  authFields,
  authParamsSchema,
  defaultUiAuthParams,
  prepareAuthData
} from '$lib/functions/kafka/authParamsSchema'
import { intersection } from '$lib/functions/valibot'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { ConnectorType, Direction } from '$lib/types/connectors'
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

import { TabKafkaAuth } from './tabs/kafka/TabKafkaAuth'
import TabkafkaOutputDetails from './tabs/kafka/TabKafkaOutputDetails'
import TabOutputFormatDetails from './tabs/TabOutputFormatDetails'
import Transition from './tabs/Transition'

const schema = intersection([
  va.object({
    name: va.nonOptional(va.string()),
    description: va.optional(va.string(), ''),
    bootstrap_servers: va.nonOptional(va.string()),
    topic: va.optional(va.string(), ''),
    format_name: va.nonOptional(va.enumType(['json', 'csv'])),
    json_array: va.nonOptional(va.boolean())
  }),
  authParamsSchema
])

export type KafkaOutputSchema = va.Input<typeof schema>

export const KafkaOutputConnectorDialog = (props: ConnectorDialogProps) => {
  const [activeTab, setActiveTab] = useState<string>('detailsTab')
  const [curValues, setCurValues] = useState<KafkaOutputSchema | undefined>(undefined)

  // Initialize the form either with values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseKafkaOutputSchema(props.connector))
    }
  }, [props.connector])

  const defaultValues: KafkaOutputSchema = {
    name: '',
    description: '',
    bootstrap_servers: '',
    topic: '',
    format_name: 'json',
    json_array: false,
    ...defaultUiAuthParams
  }

  const handleClose = () => {
    setActiveTab(tabList[0])
    props.setShow(false)
  }

  // Define what should happen when the form is submitted
  const prepareData = (data: KafkaOutputSchema) => ({
    name: data.name,
    description: data.description,
    config: {
      transport: {
        name: connectorTransportName(ConnectorType.KAFKA_OUT),
        config: {
          'bootstrap.servers': data.bootstrap_servers,
          topic: data.topic,
          ...prepareAuthData(data)
        }
      },
      format: {
        name: data.format_name,
        config:
          data.format_name === 'json'
            ? {
                array: data.json_array
              }
            : {}
      }
    }
  })

  const onSubmit = useConnectorRequest(props.connector, prepareData, props.onSuccess, handleClose)

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = (errors: FieldErrors<KafkaOutputSchema>) => {
    if (!props.show) {
      return
    }
    if (errors?.name || errors?.description) {
      setActiveTab('detailsTab')
    } else if (errors?.bootstrap_servers || errors?.topic) {
      setActiveTab('sourceTab')
    } else if (authFields.some(f => f in errors)) {
      setActiveTab('authTab')
    } else if (errors?.format_name || errors?.json_array) {
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
              {props.connector === undefined ? 'New Kafka Output' : 'Update ' + props.connector.name}
            </Typography>
            {props.connector === undefined && <Typography variant='body2'>Add a Kafka Output.</Typography>}
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
                      title='Details'
                      subtitle='Enter Details'
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
                      active={activeTab === 'sourceTab'}
                      subtitle='Sink details'
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
                <TabKafkaNameAndDesc direction={Direction.OUTPUT} />
                {tabFooter}
              </TabPanel>
              <TabPanel
                value='sourceTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                <TabkafkaOutputDetails />
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
                <TabOutputFormatDetails />
                {tabFooter}
              </TabPanel>
            </TabContext>
          </Box>
        </DialogContent>
      </FormContainer>
    </Dialog>
  )
}
