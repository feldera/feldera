// A create/update dialog for a Kafka input connector.
'use client'

import { TabKafkaAuth } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaAuth'
import TabKafkaNameAndDesc from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaNameAndDesc'
import TabkafkaOutputDetails from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaOutputDetails'
import { SnowflakeOutputFormatDetails } from '$lib/components/connectors/dialogs/tabs/snowflake/SnowflakeOutputFormatDetails'
import TabFooter from '$lib/components/connectors/dialogs/tabs/TabFooter'
import TabLabel from '$lib/components/connectors/dialogs/tabs/TabLabel'
import Transition from '$lib/components/connectors/dialogs/tabs/Transition'
import {
  connectorTransportName,
  parseSnowflakeOutputSchema,
  parseSnowflakeOutputSchemaConfig
} from '$lib/functions/connectors'
import {
  authFields,
  authParamsSchema,
  defaultUiAuthParams,
  prepareAuthData
} from '$lib/functions/kafka/authParamsSchema'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { ConnectorType, Direction } from '$lib/types/connectors'
import ConnectorDialogProps from '$lib/types/connectors/ConnectorDialogProps'
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

const schema = va.object({
  name: va.nonOptional(va.string()),
  description: va.optional(va.string(), ''),
  config: va.intersect([
    va.object({
      bootstrap_servers: va.nonOptional(va.string()),
      topic: va.nonOptional(va.string([va.minLength(1, 'Topic name should not be empty')])),
      format_name: va.nonOptional(va.enumType(['json', 'avro'])),
      update_format: va.literal('snowflake'),
      transport_user_config: va.optional(va.any()),
      format_user_config: va.optional(va.any())
    }),
    authParamsSchema
  ])
})
export type SnowflakeOutputSchema = va.Input<typeof schema>

export const SnowflakeOutputConnectorDialog = (props: ConnectorDialogProps) => {
  const [rawJSON, setRawJSON] = useState(false)
  const [activeTab, setActiveTab] = useState<string>('detailsTab')
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
    config: {
      bootstrap_servers: '',
      topic: '',
      format_name: 'json',
      update_format: 'snowflake',
      ...defaultUiAuthParams
    }
  }

  const handleClose = () => {
    setActiveTab(tabList[0])
    props.setShow(false)
  }

  // Define what should happen when the form is submitted
  const prepareData = (data: SnowflakeOutputSchema) => ({
    name: data.name,
    description: data.description,
    config: normalizeConfig(data.config)
  })

  const normalizeConfig = (data: SnowflakeOutputSchema['config']) => ({
    transport: {
      name: connectorTransportName(ConnectorType.SNOWFLAKE_OUT),
      config: {
        ...data.transport_user_config,
        'bootstrap.servers': data.bootstrap_servers,
        topic: data.topic,
        ...prepareAuthData(data)
      }
    },
    format: {
      name: data.format_name,
      config: {
        ...data.format_user_config,
        update_format: data.update_format
      }
    }
  })

  const onSubmit = useConnectorRequest(props.connector, prepareData, props.onSuccess, handleClose)

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = ({ name, description, config }: FieldErrors<SnowflakeOutputSchema>) => {
    if (!props.show) {
      return
    }
    if (name || description) {
      setActiveTab('detailsTab')
    } else if (config?.bootstrap_servers || config?.topic) {
      setActiveTab('sourceTab')
    } else if (config && authFields.some(f => f in config)) {
      setActiveTab('authTab')
    } else if (config?.format_name) {
      setActiveTab('formatTab')
    }
  }

  const tabList = ['detailsTab', 'sourceTab', 'authTab', 'formatTab']
  const tabFooter = (
    <TabFooter submitButton={props.submitButton} activeTab={activeTab} setActiveTab={setActiveTab} tabsArr={tabList} />
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
          <IconButton size='small' onClick={handleClose} sx={{ position: 'absolute', right: '1rem', top: '1rem' }}>
            <IconX />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {props.connector === undefined
                ? 'New Snowflake-Kafka Output'
                : props.existingTitle?.(props.connector.name) ?? ''}
            </Typography>
            {props.connector === undefined && (
              <Typography variant='body2'>Output to a Snowflake table via a Kafka topic</Typography>
            )}
          </Box>
          {rawJSON ? (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              {jsonSwitch}
              <GenericEditorForm
                disabled={props.disabled}
                direction={Direction.OUTPUT}
                configFromText={t => parseSnowflakeOutputSchemaConfig(JSON.parse(t))}
                configToText={c => JSON.stringify(normalizeConfig(c as any), undefined, '\t')}
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
                          icon={<IconFile />}
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
                          icon={<IconData />}
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
                          icon={<IconLockOpen />}
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
                          icon={<IconCategoryAlt />}
                        />
                      }
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
                  <TabkafkaOutputDetails disabled={props.disabled} />
                  {tabFooter}
                </TabPanel>
                <TabPanel
                  value='authTab'
                  sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
                >
                  <TabKafkaAuth disabled={props.disabled} />
                  {tabFooter}
                </TabPanel>
                <TabPanel
                  value='formatTab'
                  sx={{ border: 0, boxShadow: 0, width: '100%', backgroundColor: 'transparent' }}
                >
                  <SnowflakeOutputFormatDetails disabled={props.disabled} />
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
