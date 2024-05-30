import {
  ConnectorEditDialog,
  PlainDialogContent,
  VerticalTabsDialogContent
} from '$lib/components/connectors/dialogs/elements/DialogComponents'
import { TabDeltaLakeGeneral } from '$lib/components/connectors/dialogs/tabs/deltalake/TabDeltaLakeGeneral'
import { TabDeltaLakeOptions } from '$lib/components/connectors/dialogs/tabs/deltalake/TabDeltaLakeOptions'
import { GenericEditorForm } from '$lib/components/connectors/dialogs/tabs/GenericConnectorForm'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import {
  normalizeDeltaLakeInputConfig,
  parseConnectorDescrWith,
  parseDeltaLakeInputSchemaConfig,
  prepareDataWith
} from '$lib/functions/connectors'
import { defaultOutputBufferOptions, outputBufferConfigSchema } from '$lib/functions/connectors/outputBuffer'
import { useConnectorRequest } from '$lib/services/connectors/dialogs/SubmitHandler'
import { Direction } from '$lib/types/connectors'
import { ConnectorDialogProps } from '$lib/types/connectors/ConnectorDialogProps'
import { useState } from 'react'
import { FieldErrors } from 'react-hook-form'
import JSONbig from 'true-json-bigint'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import { FormControlLabel, Switch, Tooltip } from '@mui/material'
import Box from '@mui/material/Box'

import { DeltaLakeIngestModeElement } from './elements/deltalake/IngestModeElement'

const schema = va.merge([
  va.object({
    name: va.nonOptional(va.string([va.minLength(1, 'Specify connector name')])),
    description: va.optional(va.string(), ''),
    transport: va.nonOptional(
      va.object(
        {
          uri: va.nonOptional(va.string([va.minLength(1)])),
          filter: va.transform(va.optional(va.string([va.minLength(1)])), v => v || undefined)
        },
        va.union([va.string(), va.number(), va.boolean(), va.null_()])
      )
    )
  }),
  outputBufferConfigSchema
])

type DeltaLakeInputSchema = va.Input<typeof schema>

export const DeltaLakeInputConnectorDialog = (props: ConnectorDialogProps) => {
  const tabs = ['generalTab', 'optionsTab', 'ingestTab'] as const
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('generalTab')

  const [editorDirty, setEditorDirty] = useState<'dirty' | 'clean' | 'error'>('clean')

  const [rawJSON, setRawJSON] = useState(false)
  const jsonSwitch = (
    <Box sx={{ pl: 4 }}>
      <Tooltip title={editorDirty !== 'clean' ? 'Fix errors before switching the view' : undefined}>
        <FormControlLabel
          control={<Switch checked={rawJSON} onChange={(e, v) => setRawJSON(v)} disabled={editorDirty !== 'clean'} />}
          label='Edit JSON'
        />
      </Tooltip>
    </Box>
  )

  const tabFooter = <TabFooter submitButton={props.submitButton} {...{ activeTab, setActiveTab, tabs }} />

  const handleClose = () => {
    setActiveTab(tabs[0])
    props.setShow(false)
  }

  const defaultValues: DeltaLakeInputSchema = props.connector
    ? parseConnectorDescrWith(parseDeltaLakeInputSchemaConfig)(props.connector)
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

  const handleErrors = ({ name, description, transport }: FieldErrors<DeltaLakeInputSchema>) => {
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
        {jsonSwitch}
        <Box sx={{ height: '70vh' }}>
          {rawJSON ? (
            <PlainDialogContent submitButton={props.submitButton}>
              <GenericEditorForm
                disabled={props.disabled}
                direction={Direction.INPUT}
                configFromText={text => parseDeltaLakeInputSchemaConfig(JSONbig.parse(text))}
                configToText={config => JSONbig.stringify(normalizeDeltaLakeInputConfig(config), undefined, '\t')}
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
                  testid: 'button-tab-output-buffer',
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
