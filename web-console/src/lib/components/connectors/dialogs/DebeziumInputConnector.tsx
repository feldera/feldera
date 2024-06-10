// A create/update dialog for a Kafka input connector.
'use client'

import {
  ConnectorEditDialog,
  PlainDialogContent,
  VerticalTabsDialogContent
} from '$lib/components/connectors/dialogs/elements/DialogComponents'
import { JsonSwitch } from '$lib/components/connectors/dialogs/JSONSwitch'
import { DebeziumInputFormatDetails } from '$lib/components/connectors/dialogs/tabs/debezium/DebeziumInputFormatDetails'
import { GenericEditorForm } from '$lib/components/connectors/dialogs/tabs/GenericConnectorForm'
import { TabKafkaAuth } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaAuth'
import { TabKafkaInputDetails } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaInputDetails'
import { TabKafkaNameAndDesc } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaNameAndDesc'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import {
  normalizeDebeziumInputConfig,
  parseDebeziumInputSchema,
  parseDebeziumInputSchemaConfig,
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
        auto_offset_reset: va.optional(
          va.picklist(['smallest', 'earliest', 'beginning', 'largest', 'latest', 'end', 'error'], 'Invalid enum value'),
          'earliest'
        ),
        group_id: va.coerce(
          va.optional(va.string([va.minLength(1, 'group.id should not be empty')])),
          v => v || undefined
        ),
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
    format_name: va.nonOptional(va.picklist(['json'])),
    update_format: va.literal('debezium'),
    json_flavor: va.picklist(['debezium_mysql'])
  })
})
export type DebeziumInputSchema = va.Input<typeof schema>

export const DebeziumInputConnectorDialog = (props: ConnectorDialogProps) => {
  const tabs = ['detailsTab', 'sourceTab', 'authTab', 'formatTab'] as const

  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('detailsTab')
  const [curValues, setCurValues] = useState<DebeziumInputSchema | undefined>(undefined)

  // Initialize the form either with default or values from the passed in connector
  useEffect(() => {
    if (props.connector) {
      setCurValues(parseDebeziumInputSchema(props.connector))
    }
  }, [props.connector])

  const defaultValues: DebeziumInputSchema = {
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
      update_format: 'debezium',
      json_flavor: 'debezium_mysql'
    }
  }

  const handleClose = () => {
    setActiveTab(tabs[0])
    props.setShow(false)
  }

  const onSubmit = useConnectorRequest(
    props.connector,
    prepareDataWith(normalizeDebeziumInputConfig),
    props.onSuccess,
    handleClose
  )

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = ({ name, description, transport, format }: FieldErrors<DebeziumInputSchema>) => {
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
    } else if (format?.format_name || format?.update_format) {
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
              ? 'New Debezium Datasource'
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
                direction={Direction.INPUT}
                configFromText={text => parseDebeziumInputSchemaConfig(JSONbig.parse(text))}
                configToText={config => JSONbig.stringify(normalizeDebeziumInputConfig(config), undefined, '\t')}
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
                        direction={Direction.INPUT}
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
                  description: 'Source details',
                  icon: <i className={`bx bx-data`} style={{}} />,
                  testid: 'button-tab-server',
                  content: (
                    <>
                      <TabKafkaInputDetails disabled={props.disabled} parentName='transport' />
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
                      <DebeziumInputFormatDetails disabled={props.disabled} />
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
