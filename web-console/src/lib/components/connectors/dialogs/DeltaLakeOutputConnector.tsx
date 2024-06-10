import { DeltaLakeWriteModeElement } from '$lib/components/connectors/dialogs/elements/deltalake/WriteModeElement'
import {
  ConnectorEditDialog,
  PlainDialogContent,
  VerticalTabsDialogContent
} from '$lib/components/connectors/dialogs/elements/DialogComponents'
import { JsonSwitch } from '$lib/components/connectors/dialogs/JSONSwitch'
import { TabDeltaLakeGeneral } from '$lib/components/connectors/dialogs/tabs/deltalake/TabDeltaLakeGeneral'
import { TabDeltaLakeOptions } from '$lib/components/connectors/dialogs/tabs/deltalake/TabDeltaLakeOptions'
import { TabOutputBufferOptions } from '$lib/components/connectors/dialogs/tabs/generic/TabOutputBufferOptions'
import { GenericEditorForm } from '$lib/components/connectors/dialogs/tabs/GenericConnectorForm'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import { bignumber } from '$lib/functions/common/valibot'
import {
  normalizeDeltaLakeOutputConfig,
  parseConnectorDescrWith,
  parseDeltaLakeOutputSchemaConfig,
  prepareDataWith
} from '$lib/functions/connectors'
import { outputBufferConfigSchema, outputBufferConfigValidation } from '$lib/functions/connectors/outputBuffer'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { Direction } from '$lib/types/connectors'
import { ConnectorDialogProps } from '$lib/types/connectors/ConnectorDialogProps'
import BigNumber from 'bignumber.js/bignumber.js'
import { useState } from 'react'
import { FieldErrors } from 'react-hook-form'
import JSONbig from 'true-json-bigint'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import Box from '@mui/material/Box'

const schema = va.merge(
  [
    va.object({
      name: va.nonOptional(va.string([va.minLength(1, 'Specify connector name')])),
      description: va.optional(va.string(), ''),
      transport: va.nonOptional(
        va.object(
          {
            uri: va.nonOptional(va.string([va.minLength(1, 'Enter DeltaLake resource URI')])),
            mode: va.nonOptional(va.picklist(['append', 'truncate', 'error_if_exists']))
          },
          va.union([va.string(), va.number(), bignumber(), va.boolean(), va.null_(), va.undefined_()])
        )
      )
    }),
    outputBufferConfigSchema
  ],
  [outputBufferConfigValidation()]
)

type DeltaLakeOutputSchema = va.Input<typeof schema>

export const DeltaLakeOutputConnectorDialog = (props: ConnectorDialogProps) => {
  const tabs = ['generalTab', 'optionsTab', 'bufferTab'] as const
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('generalTab')

  const [editorDirty, setEditorDirty] = useState<'dirty' | 'clean' | 'error'>('clean')

  const [rawJSON, setRawJSON] = useState(false)

  const tabFooter = <TabFooter submitButton={props.submitButton} {...{ activeTab, setActiveTab, tabs }} />

  const handleClose = () => {
    setActiveTab(tabs[0])
    props.setShow(false)
  }

  const defaultValues: DeltaLakeOutputSchema = props.connector
    ? (parseConnectorDescrWith(parseDeltaLakeOutputSchemaConfig)(props.connector) as DeltaLakeOutputSchema)
    : {
        name: '',
        transport: {
          uri: '',
          mode: 'append'
        },
        enable_output_buffer: true,
        max_output_buffer_time_millis: new BigNumber(10000),
        max_output_buffer_size_records: new BigNumber(1000000)
      }

  const onSubmit = useConnectorRequest(
    props.connector,
    prepareDataWith(normalizeDeltaLakeOutputConfig),
    props.onSuccess,
    handleClose
  )

  const handleErrors = ({ name, description, transport }: FieldErrors<DeltaLakeOutputSchema>) => {
    if (!props.show) {
      return
    }
    if (name || description || transport?.uri) {
      setActiveTab('generalTab')
      return
    }
    if (transport) {
      setActiveTab('optionsTab')
      return
    }
  }

  return (
    <>
      <ConnectorEditDialog
        {...{
          show: props.show,
          handleClose: handleClose,
          resolver: valibotResolver(schema),
          defaultValues: defaultValues,
          onSubmit: onSubmit,
          handleErrors: handleErrors,
          dialogTitle:
            props.connector === undefined
              ? 'Register DeltaLake Sink'
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
                configFromText={text => parseDeltaLakeOutputSchemaConfig(JSONbig.parse(text))}
                configToText={config => JSONbig.stringify(normalizeDeltaLakeOutputConfig(config), undefined, '\t')}
                setEditorDirty={setEditorDirty}
              />
            </PlainDialogContent>
          ) : (
            <VerticalTabsDialogContent
              {...{ activeTab, setActiveTab, tabs }}
              tabList={[
                {
                  name: 'generalTab',
                  title: 'Name and URI',
                  description: '',
                  icon: <i className='bx bx-file' />,
                  testid: 'button-tab-config',
                  content: (
                    <>
                      <TabDeltaLakeGeneral
                        direction={Direction.OUTPUT}
                        disabled={props.disabled}
                        parentName='transport'
                      />
                      {tabFooter}
                    </>
                  )
                },
                {
                  name: 'optionsTab',
                  title: 'Options',
                  description: 'Protocol-specific options',
                  icon: <i className='bx bx-file' />,
                  testid: 'button-tab-config',
                  content: (
                    <>
                      <DeltaLakeWriteModeElement parentName='transport' />
                      <TabDeltaLakeOptions disabled={props.disabled} parentName='transport' />
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
