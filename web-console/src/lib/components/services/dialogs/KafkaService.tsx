import { GridItems } from '$lib/components/common/GridItems'
import { TabKafkaAuth } from '$lib/components/connectors/dialogs/tabs/kafka/TabKafkaAuth'
import { TabFooter } from '$lib/components/connectors/dialogs/tabs/TabFooter'
import { TabLabel } from '$lib/components/connectors/dialogs/tabs/TabLabel'
import { authFields, authParamsSchema, defaultLibrdkafkaAuthOptions } from '$lib/functions/kafka/authParamsSchema'
import {
  fromKafkaConfig,
  librdkafkaAuthOptions,
  librdkafkaDefaultValue,
  librdkafkaOptions,
  toKafkaConfig
} from '$lib/functions/kafka/librdkafkaOptions'
import { PLACEHOLDER_VALUES } from '$lib/functions/placeholders'
import { ServiceDialogProps, ServiceProps } from '$lib/types/xgressServices/ServiceDialog'
import { useState } from 'react'
import {
  FieldErrors,
  FormContainer,
  SelectElement,
  SwitchElement,
  TextFieldElement,
  useFormContext,
  useFormState,
  useWatch
} from 'react-hook-form-mui'
import Markdown from 'react-markdown'
import { JSONEditor } from 'src/lib/components/common/JSONEditor'
import { ServiceDescr } from 'src/lib/services/manager'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'
import * as va from 'valibot'
import IconCog from '~icons/bx/cog'
import IconFile from '~icons/bx/file'
import IconLockOpen from '~icons/bx/lock-open'
import IconX from '~icons/bx/x'

import { valibotResolver } from '@hookform/resolvers/valibot'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import {
  Autocomplete,
  Box,
  Dialog,
  DialogTitle,
  Grid,
  IconButton,
  Switch,
  Tab,
  TextField,
  Tooltip,
  Typography,
  useTheme
} from '@mui/material'
import Fade from '@mui/material/Fade'

const schema = va.object({
  name: va.nonOptional(va.string([va.minLength(1, 'Specify service name')])),
  description: va.optional(va.string(), ''),
  config: va.intersect([
    va.object({
      bootstrap_servers: va.nonOptional(
        va.array(va.string([va.minLength(1, 'Specify at least one server')]), [
          va.minLength(1, 'Specify at least one server')
        ]),
        'Specify at least one server'
      )
    }),
    authParamsSchema,
    va.object({}, va.union([va.string(), va.number(), va.boolean(), va.array(va.string())]))
  ])
})
export type KafkaServiceSchema = va.Output<typeof schema>

const parseKafkaServiceDescriptor = (service: ServiceDescr) => ({
  name: service.name,
  description: service.description,
  config: {
    bootstrap_servers: service.config.kafka.bootstrap_servers,
    ...fromKafkaConfig(service.config.kafka.options)
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
  const jsonSwitch = (
    <Box sx={{ pl: 2 }}>
      <Switch checked={rawJSON} onChange={(e, v) => setRawJSON(v)} /> Edit JSON
    </Box>
  )
  const defaultValues: KafkaServiceSchema = props.service
    ? parseKafkaServiceDescriptor(props.service)
    : {
        name: '',
        description: '',
        config: {
          bootstrap_servers: [''],
          ...(defaultLibrdkafkaAuthOptions as any)
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
        defaultValues={defaultValues}
        onSuccess={onSuccess}
        onError={handleErrors}
      >
        <DialogTitle sx={{ textAlign: 'center' }}>
          {props.service === undefined ? 'New Kafka Service' : props.existingTitle?.(props.service.name) ?? ''}
        </DialogTitle>
        <IconButton
          onClick={handleClose}
          sx={{
            position: 'absolute',
            right: 8,
            top: 8
          }}
        >
          <IconX />
        </IconButton>
        {jsonSwitch}
        <Box sx={{ height: '70vh' }}>
          {rawJSON ? (
            <>
              <Box sx={{ height: '100%' }}>
                <Box sx={{ display: 'flex', flexDirection: 'column', p: 4, height: '100%' }}>
                  <GenericEditorForm disabled={props.disabled}></GenericEditorForm>
                  <Box sx={{ display: 'flex', justifyContent: 'end', pt: 4 }}>{props.submitButton}</Box>
                </Box>
              </Box>
            </>
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
                            icon={<IconFile />}
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
                            icon={<IconCog />}
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
                            icon={<IconLockOpen />}
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
  .reduce(
    (acc, { name, ...o }) => ((acc[name.replaceAll('.', '_')] = o), acc),
    {} as Record<string, Omit<(typeof librdkafkaOptions)[number], 'name'>>
  )
const fieldOptionsKeys = Object.keys(fieldOptions)

const mandatoryFields = ['bootstrap_servers']

const TabKafkaConfig = (props: { disabled?: boolean }) => {
  const config = useWatch<{ config: ServiceProps['config'] }>({ name: 'config' })
  const ctx = useFormContext()
  const usedFields = Object.keys(config).filter(field => fieldOptions[field])
  const theme = useTheme()

  return (
    <>
      <Box sx={{ height: '100%', overflowY: 'auto', ml: -4, pl: 4 }}>
        <Grid container spacing={4} sx={{ height: 'auto', pr: 4, mr: -16 }}>
          {usedFields.map(field => (
            <>
              <Grid item xs={12} sm={6} display='flex' alignItems='center'>
                {mandatoryFields.includes(field) ? (
                  <Box sx={{ width: '1rem' }}></Box>
                ) : (
                  <IconButton size='small' sx={{ ml: -4 }} onClick={() => ctx.unregister('config.' + field)}>
                    <IconX></IconX>
                  </IconButton>
                )}
                <Tooltip
                  slotProps={{
                    tooltip: {
                      sx: {
                        backgroundColor: theme.palette.background.default,
                        color: theme.palette.text.primary,
                        fontSize: 14
                      }
                    }
                  }}
                  title={
                    <Markdown>
                      {
                        (optionName => librdkafkaOptions.find(option => option.name === optionName))(
                          field.replaceAll('_', '.')
                        )?.description
                      }
                    </Markdown>
                  }
                >
                  <Typography>{field.replaceAll('_', '.')}</Typography>
                </Tooltip>
              </Grid>
              <Grid item xs={12} sm={6}>
                {match(fieldOptions[field].type)
                  .with('string', () => (
                    <TextFieldElement key={field} name={'config.' + field} size='small' fullWidth></TextFieldElement>
                  ))
                  .with('number', () => (
                    <TextFieldElement
                      key={field}
                      name={'config.' + field}
                      size='small'
                      fullWidth
                      type='number'
                    ></TextFieldElement>
                  ))
                  .with('enum', () => (
                    <SelectElement
                      key={field}
                      name={'config.' + field}
                      size='small'
                      options={fieldOptions[field].range.split(', ').map(option => ({
                        id: option,
                        label: option
                      }))}
                      fullWidth
                      disabled={props.disabled}
                      inputProps={{
                        'data-testid': 'input-' + field
                      }}
                    ></SelectElement>
                  ))
                  .with('boolean', () => (
                    <SwitchElement key={field} name={'config.' + field} label={''}></SwitchElement>
                  ))
                  .with('list', () => (
                    <TextFieldElement
                      key={field}
                      multiline
                      transform={{
                        input: (v: string[]) => {
                          return v.join(', ')
                        },
                        output: (v: string) => {
                          return v.split(', ')
                        }
                      }}
                      name={'config.' + field}
                      size='small'
                      fullWidth
                      disabled={props.disabled}
                      inputProps={{
                        'data-testid': 'input-' + field
                      }}
                    />
                  ))
                  .exhaustive()}
              </Grid>
            </>
          ))}

          <Grid item xs={12} sm={6}>
            <Autocomplete
              value={null}
              inputValue={undefined}
              blurOnSelect={true}
              onChange={(e, value) => {
                if (!value) {
                  return
                }
                const field = value.replaceAll('.', '_')
                setTimeout(() => ctx.setFocus('config.' + field), 0)
                if (ctx.getValues('config.' + field)) {
                  return
                }
                const option = fieldOptions[field]
                invariant(option)
                ctx.setValue('config.' + field, librdkafkaDefaultValue(option))
              }}
              options={fieldOptionsKeys.map(option => option.replaceAll('_', '.'))}
              size='small'
              renderInput={params => <TextField placeholder='Add option' {...params} />}
            />
          </Grid>
        </Grid>
      </Box>
      <Typography sx={{ mt: 'auto', pt: 4 }}>
        See{' '}
        <a href='https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md'>librdkafka documentation</a>{' '}
        for reference
      </Typography>
    </>
  )
}

const GenericEditorForm = (props: { disabled?: boolean }) => (
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

export const ServiceConfigEditorElement = (props: { disabled?: boolean }) => {
  const ctx = useFormContext()
  const config: Record<string, unknown> = ctx.watch('config')
  const { errors } = useFormState({ control: ctx.control })
  return (
    <JSONEditor
      disabled={props.disabled}
      valueFromText={text => fromKafkaConfig(JSON.parse(text))}
      valueToText={config => JSON.stringify(toKafkaConfig(config as any), undefined, '\t')}
      errors={(errors?.config as any) ?? {}}
      value={config}
      setValue={config => {
        ctx.setValue('config', config)
      }}
      data-testid='input-config'
    ></JSONEditor>
  )
}
