import { DeltaLakeIngestModeElement } from '$lib/components/connectors/dialogs/elements/deltalake/IngestModeElement'
import {
  ConnectorEditDialog,
  PlainDialogContent,
  VerticalTabsDialogContent
} from '$lib/components/connectors/dialogs/elements/DialogComponents'
import { JsonSwitch } from '$lib/components/connectors/dialogs/JSONSwitch'
import { TabDeltaLakeGeneral } from '$lib/components/connectors/dialogs/tabs/deltalake/TabDeltaLakeGeneral'
import { TabDeltaLakeOptions } from '$lib/components/connectors/dialogs/tabs/deltalake/TabDeltaLakeOptions'
import { GenericEditorForm } from '$lib/components/connectors/dialogs/tabs/GenericConnectorForm'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import { bignumber } from '$lib/functions/common/valibot'
import {
  normalizeDeltaLakeInputConfig,
  parseConnectorDescrWith,
  parseDeltaLakeInputSchemaConfig,
  prepareDataWith
} from '$lib/functions/connectors'
import { defaultOutputBufferOptions } from '$lib/functions/connectors/outputBuffer'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { Direction } from '$lib/types/connectors'
import { ConnectorDialogProps } from '$lib/types/connectors/ConnectorDialogProps'
import { useState } from 'react'
import { FieldErrors } from 'react-hook-form'
import JSONbig from 'true-json-bigint'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import Box from '@mui/material/Box'

const restEntries = va.union([va.string(), va.number(), bignumber(), va.boolean(), va.null_(), va.undefined_()])

const commonEntries = {
  uri: va.nonOptional(va.string([va.minLength(1, 'Enter DeltaLake storage URI')]), 'Enter DeltaLake storage URI')
}

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify connector name')])),
  description: va.optional(va.string(), ''),
  transport: va.nonOptional(
    va.variant('mode', [
      va.object(
        {
          ...commonEntries,
          mode: va.literal('snapshot'),
          snapshot_filter: va.transform(va.optional(va.string()), v => v || undefined),
          timestamp_column: va.transform(va.optional(va.string()), v => v || undefined)
        },
        restEntries
      ),
      va.object(
        {
          ...commonEntries,
          mode: va.literal('follow'),
          snapshot_filter: va.transform(va.any(), () => undefined),
          timestamp_column: va.transform(va.any(), () => undefined)
        },
        restEntries
      ),
      va.object(
        {
          ...commonEntries,
          mode: va.literal('snapshot_and_follow'),
          snapshot_filter: va.transform(va.optional(va.string()), v => v || undefined),
          timestamp_column: va.transform(va.optional(va.string()), v => v || undefined)
        },
        restEntries
      )
    ])
  )
})

type DeltaLakeInputSchema = va.Input<typeof schema>

export const DeltaLakeInputConnectorDialog = (props: ConnectorDialogProps) => {
  const tabs = ['generalTab', 'optionsTab', 'ingestTab'] as const
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('generalTab')

  const [editorDirty, setEditorDirty] = useState<'dirty' | 'clean' | 'error'>('clean')

  const [rawJSON, setRawJSON] = useState(false)

  const tabFooter = <TabFooter submitButton={props.submitButton} {...{ activeTab, setActiveTab, tabs }} />

  const handleClose = () => {
    setActiveTab(tabs[0])
    props.setShow(false)
  }

  const defaultValues: DeltaLakeInputSchema = props.connector
    ? (parseConnectorDescrWith(parseDeltaLakeInputSchemaConfig)(props.connector) as unknown as DeltaLakeInputSchema)
    : {
        name: '',
        transport: {
          uri: '',
          mode: 'snapshot'
        },
        ...defaultOutputBufferOptions
      }

  const onSubmit = useConnectorRequest(
    props.connector,
    prepareDataWith(normalizeDeltaLakeInputConfig),
    props.onSuccess,
    handleClose
  )

  const handleErrors = (errors: FieldErrors<DeltaLakeInputSchema>) => {
    const { name, description, transport } = errors
    if (!props.show) {
      return
    }
    if (name || description || transport?.uri) {
      setActiveTab('generalTab')
      return
    }
    if (transport && !(transport?.mode || transport?.filter || transport?.version || transport?.datetime)) {
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
              ? 'Register DeltaLake Source'
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
                configFromText={text => parseDeltaLakeInputSchemaConfig(JSONbig.parse(text))}
                configToText={config =>
                  JSONbig.stringify(
                    normalizeDeltaLakeInputConfig(va.safeParse(schema, config).output as any),
                    undefined,
                    '\t'
                  )
                }
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
                        direction={Direction.INPUT}
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
                      <TabDeltaLakeOptions disabled={props.disabled} parentName='transport' />
                      {tabFooter}
                    </>
                  )
                },
                {
                  name: 'ingestTab',
                  title: 'Ingest mode',
                  description: 'Delta table read mode',
                  icon: <i className='bx bx-exit' />,
                  testid: 'button-tab-ingest-mode',
                  content: (
                    <>
                      <DeltaLakeIngestModeElement parentName='transport' />
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
