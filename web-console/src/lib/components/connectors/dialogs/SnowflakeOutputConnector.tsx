// A create/update dialog for a Kafka input connector.
'use client'

import {
  ConnectorEditDialog,
  PlainDialogContent,
  VerticalTabsDialogContent
} from '$lib/components/connectors/dialogs/elements/DialogComponents'
import { JsonSwitch } from '$lib/components/connectors/dialogs/JSONSwitch'
import { TabOutputBufferOptions } from '$lib/components/connectors/dialogs/tabs/generic/TabOutputBufferOptions'
import { GenericEditorForm } from '$lib/components/connectors/dialogs/tabs/GenericConnectorForm'
import { TabKafkaAuth } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaAuth'
import { TabKafkaNameAndDesc } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaNameAndDesc'
import { TabKafkaOutputDetails } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaOutputDetails'
import { SnowflakeOutputFormatDetails } from '$lib/components/connectors/dialogs/tabs/snowflake/SnowflakeOutputFormatDetails'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import {
  normalizeSnowflakeOutputConfig,
  parseSnowflakeOutputSchema,
  parseSnowflakeOutputSchemaConfig,
  prepareDataWith
} from '$lib/functions/connectors'
import { authFields, authParamsSchema, defaultLibrdkafkaAuthOptions } from '$lib/functions/kafka/authParamsSchema'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { Direction } from '$lib/types/connectors'
import { ConnectorDialogProps } from '$lib/types/connectors/ConnectorDialogProps'
import { useEffect, useState } from 'react'
import { FieldErrors } from 'react-hook-form'
import JSONbig from 'true-json-bigint'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import Box from '@mui/material/Box'

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
  const tabs = ['detailsTab', 'sourceTab', 'authTab', 'formatTab', 'bufferTab'] as const

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

  const onSubmit = useConnectorRequest(
    props.connector,
    prepareDataWith(normalizeSnowflakeOutputConfig),
    props.onSuccess,
    handleClose
  )

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

  const [editorDirty, setEditorDirty] = useState<'dirty' | 'clean' | 'error'>('clean')
  const [rawJSON, setRawJSON] = useState(false)

  const tabFooter = <TabFooter submitButton={props.submitButton} {...{ activeTab, setActiveTab, tabs }} />

  return (
    <>
      <ConnectorEditDialog
        {...{
          show: props.show,
          handleClose: handleClose,
          resolver: valibotResolver(schema),
          values: curValues,
          defaultValues: defaultValues,
          onSubmit: onSubmit,
          handleErrors: handleErrors,
          dialogTitle:
            props.connector === undefined
              ? 'New Snowflake-Kafka Output'
              : props.existingTitle?.(props.connector.name) ?? '',
          submitButton: props.submitButton,
          tabs,
          activeTab,
          setActiveTab
        }}
      >
        <JsonSwitch {...{ rawJSON, setRawJSON, editorDirty }}></JsonSwitch>
        <Box sx={{ height: '70vh' }}>
          {rawJSON ? (
            <PlainDialogContent submitButton={props.submitButton}>
              <GenericEditorForm
                disabled={props.disabled}
                direction={Direction.OUTPUT}
                configFromText={text => parseSnowflakeOutputSchemaConfig(JSONbig.parse(text))}
                configToText={config => JSONbig.stringify(normalizeSnowflakeOutputConfig(config), undefined, '\t')}
                setEditorDirty={setEditorDirty}
              />
            </PlainDialogContent>
          ) : (
            <VerticalTabsDialogContent
              {...{ activeTab, setActiveTab, tabs }}
              tabList={[
                {
                  name: 'detailsTab',
                  title: 'Metadata',
                  description: 'Description',
                  icon: <i className={`bx bx-file`} style={{}} />,
                  testid: 'button-tab-name',
                  content: (
                    <>
                      <TabKafkaNameAndDesc
                        direction={Direction.OUTPUT}
                        disabled={props.disabled}
                        parentName='transport'
                      />
                      {tabFooter}
                    </>
                  )
                },
                {
                  name: 'sourceTab',
                  title: 'Server',
                  description: 'Sink details',
                  icon: <i className={`bx bx-data`} style={{}} />,
                  testid: 'button-tab-server',
                  content: (
                    <>
                      <TabKafkaOutputDetails disabled={props.disabled} parentName='transport' />
                      {tabFooter}
                    </>
                  )
                },
                {
                  name: 'authTab',
                  title: 'Security',
                  description: 'Authentication protocol',
                  icon: <i className={`bx bx-lock-open`} style={{}} />,
                  testid: 'button-tab-auth',
                  content: (
                    <>
                      <TabKafkaAuth disabled={props.disabled} parentName={'transport'} />
                      {tabFooter}
                    </>
                  )
                },
                {
                  name: 'formatTab',
                  title: 'Format',
                  description: 'Data details',
                  icon: <i className={`bx bx-category-alt`} style={{}} />,
                  testid: 'button-tab-format',
                  content: (
                    <>
                      <SnowflakeOutputFormatDetails disabled={props.disabled} />
                      {tabFooter}
                    </>
                  )
                },
                {
                  name: 'bufferTab',
                  title: 'Output buffer',
                  description: 'Duration and capacity configuration',
                  icon: <i className='bx bx-align-left' />,
                  testid: 'button-tab-output-buffer',
                  content: (
                    <>
                      <TabOutputBufferOptions disabled={props.disabled} />
                      {tabFooter}
                    </>
                  )
                }
              ]}
            ></VerticalTabsDialogContent>
          )}
        </Box>
      </ConnectorEditDialog>
    </>
  )
}
