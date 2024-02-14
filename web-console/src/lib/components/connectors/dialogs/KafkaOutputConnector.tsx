// A create/update dialog for a Kafka output connector.
'use client'

import { TabKafkaNameAndDesc } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaNameAndDesc'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import { TabLabel } from '$lib/components/connectors/dialogs/tabs/TabLabel'
import { connectorTransportName, parseKafkaOutputSchema, parseKafkaOutputSchemaConfig } from '$lib/functions/connectors'
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
import { Switch } from '@mui/material'
import Box from '@mui/material/Box'
import Dialog from '@mui/material/Dialog'
import DialogContent from '@mui/material/DialogContent'
import IconButton from '@mui/material/IconButton'
import Tab from '@mui/material/Tab'
import Typography from '@mui/material/Typography'

import { GenericEditorForm } from './tabs/GenericConnectorForm'
import { TabKafkaAuth } from './tabs/kafka/TabKafkaAuth'
import { TabKafkaOutputDetails } from './tabs/kafka/TabKafkaOutputDetails'
import TabOutputFormatDetails from './tabs/TabOutputFormatDetails'
import Transition from './tabs/Transition'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify connector name')])),
  description: va.optional(va.string(), ''),
  transport: va.intersect([
    va.object({
      bootstrap_servers: va.nonOptional(
        va.array(va.string([va.minLength(1, 'Specify at least one server')]), [
          va.minLength(1, 'Specify at least one server')
        ])
      ),
      topic: va.optional(va.string(), '')
    }),
    authParamsSchema
  ]),
  format: va.object({
    format_name: va.nonOptional(va.picklist(['json', 'csv'])),
    json_array: va.nonOptional(va.boolean())
  })
})

export type KafkaOutputSchema = va.Input<typeof schema>

export const KafkaOutputConnectorDialog = (props: ConnectorDialogProps) => {
  const tabs = ['detailsTab', 'sourceTab', 'authTab', 'formatTab'] as const
  const [rawJSON, setRawJSON] = useState(false)
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('detailsTab')
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
    transport: {
      bootstrap_servers: [''],
      topic: '',
      ...defaultLibrdkafkaAuthOptions
    },
    format: {
      format_name: 'json',
      json_array: false
    }
  }

  const handleClose = () => {
    setActiveTab(tabs[0])
    props.setShow(false)
  }

  // Define what should happen when the form is submitted
  const prepareData = (data: KafkaOutputSchema) => ({
    name: data.name,
    description: data.description,
    config: normalizeConfig(data)
  })

  const normalizeConfig = (data: {
    transport: Record<string, LibrdkafkaOptionType>
    format: Record<string, string | boolean>
  }) => ({
    transport: {
      name: connectorTransportName(ConnectorType.KAFKA_OUT),
      config: toKafkaConfig(data.transport)
    },
    format: {
      name: data.format.format_name,
      config: {
        ...(data.format.format_name === 'json'
          ? {
              array: data.format.json_array
            }
          : {})
      }
    }
  })

  const onSubmit = useConnectorRequest(props.connector, prepareData, props.onSuccess, handleClose)

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = ({ name, description, transport, format }: FieldErrors<KafkaOutputSchema>) => {
    if (!props.show) {
      return
    }
    if (name || description) {
      setActiveTab('detailsTab')
    } else if (transport?.bootstrap_servers || transport?.topic) {
      setActiveTab('sourceTab')
    } else if (transport && authFields.some(f => f in transport)) {
      setActiveTab('authTab')
    } else if (format?.format_name || format?.json_array) {
      setActiveTab('formatTab')
    }
  }

  const tabFooter = (
    <TabFooter submitButton={props.submitButton} activeTab={activeTab} setActiveTab={setActiveTab} tabs={tabs} />
  )

  const jsonSwitch = (
    <Box sx={{ pl: 2, marginTop: { xs: '0', md: '-4rem' } }}>
      <Switch checked={rawJSON} onChange={(e, v) => setRawJSON(v)} /> Edit JSON
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
            <IconX />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {props.connector === undefined ? 'New Kafka Output' : props.existingTitle?.(props.connector.name) ?? ''}
            </Typography>
            {props.connector === undefined && <Typography variant='body2'>Add a Kafka Output.</Typography>}
          </Box>
          {rawJSON ? (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              {jsonSwitch}
              <GenericEditorForm
                disabled={props.disabled}
                direction={Direction.OUTPUT}
                configFromText={text => parseKafkaOutputSchemaConfig(JSON.parse(text))}
                configToText={config => JSON.stringify(normalizeConfig(config), undefined, '\t')}
              />
              <Box sx={{ display: 'flex', justifyContent: 'end' }}>{props.submitButton}</Box>
            </Box>
          ) : (
            <Box sx={{ display: 'flex', flexWrap: { xs: 'wrap', md: 'nowrap' } }}>
              <TabContext value={activeTab}>
                <Box>
                  {jsonSwitch}
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
                          title='Details'
                          subtitle='Enter Details'
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
                          active={activeTab === 'sourceTab'}
                          subtitle='Sink details'
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
                          active={activeTab === 'formatTab'}
                          subtitle='Data details'
                          icon={<IconCategoryAlt />}
                        />
                      }
                      data-testid='button-tab-format'
                    />
                  </TabList>
                </Box>
                <TabPanel
                  value='detailsTab'
                  sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
                >
                  <TabKafkaNameAndDesc direction={Direction.OUTPUT} disabled={props.disabled} />
                  {tabFooter}
                </TabPanel>
                <TabPanel
                  value='sourceTab'
                  sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
                >
                  <TabKafkaOutputDetails disabled={props.disabled} parentName='transport' />
                  {tabFooter}
                </TabPanel>
                <TabPanel
                  value='authTab'
                  sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
                >
                  <TabKafkaAuth disabled={props.disabled} parentName={'transport'} />
                  {tabFooter}
                </TabPanel>
                <TabPanel
                  value='formatTab'
                  sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
                >
                  <TabOutputFormatDetails disabled={props.disabled} />
                  {tabFooter}
                </TabPanel>
              </TabContext>
            </Box>
          )}
        </DialogContent>
      </FormContainer>
    </Dialog>
  )
}
