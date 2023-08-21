// A create/update dialog for a Kafka input connector.

import TabFooter from '$lib/components/connectors/dialogs/tabs/TabFooter'
import TabKafkaInputDetails from '$lib/components/connectors/dialogs/tabs/TabKafkaInputDetails'
import TabKafkaNameAndDesc from '$lib/components/connectors/dialogs/tabs/TabKafkaNameAndDesc'
import TabLabel from '$lib/components/connectors/dialogs/tabs/TabLabel'
import { ConnectorFormNewRequest, ConnectorFormUpdateRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { connectorTypeToConfig, connectorTypeToIcon, parseKafkaInputSchema } from '$lib/functions/connectors'
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
import { useForm } from 'react-hook-form'
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

const schema = yup.object().shape({
  name: yup.string().required(),
  description: yup.string().default(''),
  host: yup.string().required(),
  auto_offset: yup.string().default('none'),
  topics: yup.array().of(yup.string().required()).required(),
  format_name: yup.string().required().oneOf(['json', 'csv']),
  json_update_format: yup.string().oneOf(['raw', 'insert_delete']).default('raw'),
  json_array: yup.bool().required()
})

export type KafkaInputSchema = yup.InferType<typeof schema>

export const KafkaInputConnectorDialog = (props: ConnectorDialogProps) => {
  const [activeTab, setActiveTab] = useState<string>('detailsTab')
  const [curValues, setCurValues] = useState<KafkaInputSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseKafkaInputSchema(props.connector))
    }
  }, [props.connector])

  const {
    control,
    reset,
    handleSubmit,
    watch,
    formState: { errors }
  } = useForm<KafkaInputSchema>({
    resolver: yupResolver(schema),
    defaultValues: {
      name: '',
      description: '',
      host: '',
      auto_offset: 'earliest',
      topics: [],
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
    data: KafkaInputSchema,
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
            name: connectorTypeToConfig(ConnectorType.KAFKA_IN),
            config: {
              'bootstrap.servers': data.host,
              'auto.offset.reset': data.auto_offset,
              topics: data.topics
            }
          },
          format: format
        }
      }
    ]
  }
  const newRequest = (data: KafkaInputSchema): [undefined, NewConnectorRequest] => {
    return genericRequest(data) as [undefined, NewConnectorRequest]
  }
  const updateRequest = (data: KafkaInputSchema): [ConnectorId, UpdateConnectorRequest] => {
    return genericRequest(data, props.connector?.connector_id) as [ConnectorId, UpdateConnectorRequest]
  }
  const onSubmit =
    props.connector === undefined
      ? ConnectorFormNewRequest<KafkaInputSchema>(onFormSubmitted, newRequest)
      : ConnectorFormUpdateRequest<KafkaInputSchema>(onFormSubmitted, updateRequest)

  // If there is an error, switch to the earliest tab with an error
  useEffect(() => {
    if ((errors?.name || errors?.description) && props.show) {
      setActiveTab('detailsTab')
    } else if ((errors?.host || errors?.topics || errors?.auto_offset) && props.show) {
      setActiveTab('sourceTab')
    } else if ((errors?.format_name || errors?.json_array || errors?.json_update_format) && props.show) {
      setActiveTab('formatTab')
    }
  }, [props.show, errors])

  const tabList = ['detailsTab', 'sourceTab', 'formatTab']
  return (
    <Dialog
      fullWidth
      open={props.show}
      scroll='body'
      maxWidth='md'
      onClose={handleClose}
      TransitionComponent={Transition}
    >
      <form id='create-kafka-input' onSubmit={handleSubmit(onSubmit)}>
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
                      active={activeTab === 'sourceTab'}
                      subtitle='Source details'
                      icon={<Icon icon='bx:data' />}
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
                {/* @ts-ignore: TODO: This type mismatch seems like a bug in hook-form and/or resolvers */}
                <TabKafkaNameAndDesc control={control} errors={errors} />
                <TabFooter
                  isUpdate={props.connector !== undefined}
                  activeTab={activeTab}
                  setActiveTab={setActiveTab}
                  formId='create-kafka-input'
                  tabsArr={tabList}
                />
              </TabPanel>
              <TabPanel
                value='sourceTab'
                sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
              >
                <TabKafkaInputDetails control={control} errors={errors} />
                <TabFooter
                  isUpdate={props.connector !== undefined}
                  activeTab={activeTab}
                  setActiveTab={setActiveTab}
                  formId='create-kafka-input'
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
                  formId='create-kafka-input'
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

export const AddKafkaInputConnectorCard = () => {
  return (
    <AddConnectorCard
      icon={connectorTypeToIcon(ConnectorType.KAFKA_IN)}
      title='Add a Kafka Input.'
      dialog={KafkaInputConnectorDialog}
    />
  )
}
