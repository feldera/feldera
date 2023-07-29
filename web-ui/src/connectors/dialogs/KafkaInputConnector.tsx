// A create/update dialog for a Kafka input connector.

import { useState, useEffect } from 'react'
import Box from '@mui/material/Box'
import Tab from '@mui/material/Tab'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import Dialog from '@mui/material/Dialog'
import TabContext from '@mui/lab/TabContext'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'
import DialogContent from '@mui/material/DialogContent'
import { useForm } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'
import * as yup from 'yup'
import { Icon } from '@iconify/react'

import TabKafkaNameAndDesc from 'src/connectors/dialogs/tabs/TabKafkaNameAndDesc'
import TabKafkaInputDetails from 'src/connectors/dialogs/tabs/TabKafkaInputDetails'
import TabFooter from 'src/connectors/dialogs/tabs/TabFooter'
import TabLabel from 'src/connectors/dialogs/tabs/TabLabel'
import { ConnectorDescr, ConnectorId, NewConnectorRequest, UpdateConnectorRequest } from 'src/types/manager'
import Transition from './tabs/Transition'
import { ConnectorFormUpdateRequest, ConnectorFormNewRequest } from './SubmitHandler'
import { connectorTypeToConfig, parseKafkaInputSchema, ConnectorType } from 'src/types/connectors'
import { AddConnectorCard } from './AddConnectorCard'
import ConnectorDialogProps from './ConnectorDialogProps'

const schema = yup.object().shape({
  name: yup.string().required(),
  description: yup.string().default(''),
  host: yup.string().required(),
  auto_offset: yup.string().default('none'),
  topics: yup.array().of(yup.string().required()).required()
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
    formState: { errors }
  } = useForm<KafkaInputSchema>({
    resolver: yupResolver(schema),
    defaultValues: {
      name: '',
      description: '',
      host: '',
      auto_offset: 'earliest',
      topics: []
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
          format: { name: 'csv' }
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
    }
  }, [props.show, errors])

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
                      subtitle='Source details'
                      icon={<Icon icon='bx:data' />}
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
                  tabsArr={['detailsTab', 'sourceTab']}
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
                  tabsArr={['detailsTab', 'sourceTab']}
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
  return <AddConnectorCard icon='logos:kafka' title='Add a Kafka Input.' dialog={KafkaInputConnectorDialog} />
}
