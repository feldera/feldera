import { GridItems } from '$lib/components/common/GridItems'
import { TabKafkaAuth } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaAuth'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import { TabLabel } from '$lib/components/connectors/dialogs/tabs/TabLabel'
import { LibrdkafkaOptionsElement } from '$lib/components/services/dialogs/elements/LibrdkafkaOptionsElement'
import { authFields, authParamsSchema, defaultLibrdkafkaAuthOptions } from '$lib/functions/kafka/authParamsSchema'
import {
  fromLibrdkafkaConfig,
  librdkafkaAuthOptions,
  librdkafkaNonAuthFieldsSchema,
  LibrdkafkaOptions,
  librdkafkaOptions,
  toLibrdkafkaConfig
} from '$lib/functions/kafka/librdkafkaOptions'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { ServiceDescr } from '$lib/services/manager'
import { ServiceDialogProps } from '$lib/types/xgressServices/ServiceDialog'
import { Dispatch, SetStateAction, useState } from 'react'
import { FieldErrors, FormContainer, TextFieldElement, useFormContext, useFormState } from 'react-hook-form-mui'
import { JSONEditor } from 'src/lib/components/common/JSONEditor'
import JSONbig from 'true-json-bigint'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { Box, Dialog, DialogTitle, FormControlLabel, Grid, IconButton, Switch, Tab, Tooltip } from '@mui/material'
import Fade from '@mui/material/Fade'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify service name')])),
  description: va.nonOptional(va.string()),
  config: va.intersect([
    librdkafkaNonAuthFieldsSchema,
    va.object({
      bootstrap_servers: va.nonOptional(
        va.array(va.string([va.minLength(1, 'Specify at least one server')]), [
          va.minLength(1, 'Specify at least one server')
        ]),
        'Specify at least one server'
      )
    }),
    authParamsSchema
  ])
})
export type KafkaServiceSchema = va.Input<typeof schema>

const parseKafkaServiceDescriptor = (service: ServiceDescr) => ({
  name: service.name,
  description: service.description,
  config: {
    bootstrap_servers: service.config.kafka.bootstrap_servers,
    ...defaultLibrdkafkaAuthOptions,
    ...fromLibrdkafkaConfig(service.config.kafka.options)
  }
})

export const KafkaServiceDialog = (props: ServiceDialogProps) => {
  const [rawJSON, setRawJSON] = useState(false)
  const tabs = ['nameTab', 'configTab', 'authTab'] as const
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('nameTab')
  const handleClose = () => {
    props.setShow(false)
  }
  const tabFooter = <TabFooter submitButton={props.submitButton} {...{ activeTab, setActiveTab, tabs }} />
  const [editorDirty, setEditorDirty] = useState<'dirty' | 'clean' | 'error'>('clean')
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
  const defaultValues: KafkaServiceSchema = props.service
    ? parseKafkaServiceDescriptor(props.service)
    : {
        name: '',
        description: '',
        config: {
          bootstrap_servers: [''],
          ...defaultLibrdkafkaAuthOptions
        }
      }

  // If there is an error, switch to the earliest tab with an error
  const handleErrors = ({ name, description, config }: FieldErrors<KafkaServiceSchema>) => {
    if (!props.show) {
      return
    }
    if (name || description) {
      setActiveTab('nameTab')
    } else if (config && authFields.some(f => f in config)) {
      setActiveTab('authTab')
    } else if (config) {
      setActiveTab('configTab')
    }
  }

  const onSuccess = (form: KafkaServiceSchema) => {
    props.onSubmit('kafka', form)
    handleClose()
  }

  return (
    <Dialog fullWidth open={props.show} scroll='body' maxWidth='md' onClose={handleClose} TransitionComponent={Fade}>
      <FormContainer
        resolver={valibotResolver(schema)}
        mode='onChange'
        defaultValues={defaultValues}
        onSuccess={onSuccess}
        onError={handleErrors}
      >
        <DialogTitle sx={{ textAlign: 'center' }}>
          {props.service === undefined ? 'Register Kafka Service' : props.existingTitle?.(props.service.name) ?? ''}
        </DialogTitle>
        <IconButton
          onClick={handleClose}
          sx={{
            position: 'absolute',
            right: '1rem',
            top: '1rem'
          }}
          data-testid='button-close-modal'
        >
          <i className={`bx bx-x`} style={{}} />
        </IconButton>
        {jsonSwitch}
        <Box sx={{ height: '70vh' }}>
          {rawJSON ? (
            <Box sx={{ height: '100%' }}>
              <Box sx={{ display: 'flex', flexDirection: 'column', p: 4, height: '100%' }}>
                <GenericEditorForm disabled={props.disabled} setEditorDirty={setEditorDirty}></GenericEditorForm>
                <Box sx={{ display: 'flex', justifyContent: 'end', pt: 4 }}>{props.submitButton}</Box>
              </Box>
            </Box>
          ) : (
            <>
              <TabContext value={activeTab}>
                <Box sx={{ display: 'flex', flexWrap: { xs: 'wrap', md: 'nowrap' }, height: '100%' }}>
                  <Box>
                    <TabList
                      orientation='vertical'
                      onChange={(e, newValue: (typeof tabs)[number]) => setActiveTab(newValue)}
                      sx={{
                        border: 0,
                        m: 0,
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
                        value='nameTab'
                        label={
                          <TabLabel
                            title='Metadata'
                            subtitle='Name and description'
                            active={activeTab === 'nameTab'}
                            icon={<i className={`bx bx-file`} style={{}} />}
                          />
                        }
                        data-testid='button-tab-name'
                      />
                      <Tab
                        disableRipple
                        value='configTab'
                        label={
                          <TabLabel
                            title='Configuration'
                            subtitle='Data details'
                            active={activeTab === 'configTab'}
                            icon={<i className={`bx bx-cog`} style={{}} />}
                          />
                        }
                        data-testid='button-tab-config'
                      />
                      <Tab
                        disableRipple
                        value='authTab'
                        label={
                          <TabLabel
                            title='Security'
                            subtitle='Authentication protocol'
                            active={activeTab === 'authTab'}
                            icon={<i className={`bx bx-lock-open`} style={{}} />}
                          />
                        }
                        data-testid='button-tab-auth'
                      />
                    </TabList>
                  </Box>

                  <Box sx={{ width: '100%' }}>
                    <TabPanel
                      value='nameTab'
                      sx={{ border: 0, boxShadow: 0, p: 4, height: '100%', alignItems: 'start' }}
                    >
                      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                        <TabNameAndDesc disabled={props.disabled} />
                        {tabFooter}
                      </Box>
                    </TabPanel>
                    <TabPanel
                      value='configTab'
                      sx={{ border: 0, boxShadow: 0, p: 4, height: '100%', alignItems: 'start' }}
                    >
                      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                        <TabKafkaConfig disabled={props.disabled} />
                        {tabFooter}
                      </Box>
                    </TabPanel>
                    <TabPanel
                      value='authTab'
                      sx={{ border: 0, boxShadow: 0, p: 4, height: '100%', alignItems: 'start' }}
                    >
                      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                        <TabKafkaAuth disabled={props.disabled} parentName={'config'} />
                        {tabFooter}
                      </Box>
                    </TabPanel>
                  </Box>
                </Box>
              </TabContext>
            </>
          )}
        </Box>
      </FormContainer>
    </Dialog>
  )
}

const TabNameAndDesc = (props: { disabled?: boolean }) => {
  return (
    <Grid container spacing={4} sx={{}}>
      <GridItems xs={12}>
        <TextFieldElement
          name='name'
          label={'Service Name'}
          size='small'
          fullWidth
          placeholder={PLACEHOLDER_VALUES['service_name']}
          aria-describedby='validation-name'
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-datasource-name'
          }}
        />
        <TextFieldElement
          name='description'
          label='Description'
          size='small'
          fullWidth
          placeholder={PLACEHOLDER_VALUES['service_description']}
          aria-describedby='validation-description'
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-datasource-description'
          }}
        />
      </GridItems>
    </Grid>
  )
}

const fieldOptions = librdkafkaOptions
  .filter(o => o.scope === '*')
  .filter(o => !librdkafkaAuthOptions.includes(o.name as any))
  .reduce((acc, o) => ((acc[o.name.replaceAll('.', '_')] = o), acc), {} as Record<string, LibrdkafkaOptions>)

const requiredFields = ['bootstrap_servers']

const TabKafkaConfig = (props: { disabled?: boolean }) => {
  return (
    <LibrdkafkaOptionsElement
      parentName={'config'}
      fieldOptions={fieldOptions}
      requiredFields={requiredFields}
      disabled={props.disabled}
    ></LibrdkafkaOptionsElement>
  )
}

const GenericEditorForm = (props: {
  disabled?: boolean
  setEditorDirty?: Dispatch<SetStateAction<'dirty' | 'clean' | 'error'>>
}) => (
  <Grid container spacing={4} sx={{ height: '100%' }}>
    <Grid item sm={4} xs={12}>
      <TextFieldElement
        name='name'
        label={'Service name'}
        size='small'
        fullWidth
        placeholder={PLACEHOLDER_VALUES['service_name']}
        aria-describedby='validation-name'
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-service-name'
        }}
      />
    </Grid>
    <Grid item sm={8} xs={12}>
      <TextFieldElement
        name='description'
        label='Description'
        size='small'
        fullWidth
        placeholder={PLACEHOLDER_VALUES['service_description']}
        aria-describedby='validation-description'
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-service-description'
        }}
      />
    </Grid>
    <Grid item sm={12} xs={12}>
      <ServiceConfigEditorElement {...props}></ServiceConfigEditorElement>
    </Grid>
  </Grid>
)

export const ServiceConfigEditorElement = (props: {
  disabled?: boolean
  setEditorDirty?: Dispatch<SetStateAction<'dirty' | 'clean' | 'error'>>
}) => {
  const ctx = useFormContext()
  const config: Record<string, unknown> = ctx.watch('config')
  const { errors } = useFormState({ control: ctx.control })
  return (
    <JSONEditor
      disabled={props.disabled}
      valueFromText={text => fromLibrdkafkaConfig(JSONbig.parse(text))}
      valueToText={config => JSONbig.stringify(toLibrdkafkaConfig(config as any), undefined, '\t')}
      errors={(errors?.config as Record<string, { message: string }>) ?? {}}
      value={config}
      setValue={config => {
        ctx.setValue('config', config)
      }}
      setEditorDirty={props.setEditorDirty}
      data-testid='input-config'
    ></JSONEditor>
  )
}
