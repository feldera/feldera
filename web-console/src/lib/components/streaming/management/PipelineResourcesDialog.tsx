import { NumberElement } from '$lib/components/input/NumberInput'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { mutationUpdatePipeline } from '$lib/services/pipelineManagerQuery'
import { format } from 'numerable'
import { Dispatch, ReactNode, SetStateAction } from 'react'
import { FormContainer, SwitchElement, TextFieldElement, useWatch } from 'react-hook-form-mui'
import { TwoSeventyRingWithBg } from 'react-svg-spinners'
import { SliderElement } from 'src/lib/functions/common/react-hook-form-mui'
import invariant from 'tiny-invariant'
import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'
import { Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, Fade, Grid, Typography } from '@mui/material'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const workersRange = { min: 1, max: 64 }
const cpuCoresRange = { min: 0, max: 64 }
const memoryMbRange = { min: 100, max: 64000 }
const storageMbRange = { min: 1000, max: 1000000 }

function vaValueRange<T extends string | number | bigint | boolean | Date>({
  min,
  max
}: {
  min: T
  max: T
}): va.Pipe<T> {
  return [(va.minValue(min), va.maxValue(max))]
}

/** @see '$lib/services/manager/models/ResourceConfig' */
const pipelineConfigSchema = va.object({
  workers: va.optional(va.number(vaValueRange(workersRange))),
  storage: va.optional(va.boolean()),
  resources: va.optional(
    va.partial(
      va.object({
        cpu_cores_max: va.transform(va.nullish(va.number(vaValueRange(cpuCoresRange))), v => v || null),
        cpu_cores_min: va.transform(va.nullish(va.number(vaValueRange(cpuCoresRange))), v => v || null),
        memory_mb_max: va.transform(va.nullish(va.number(vaValueRange(memoryMbRange))), v => v || null),
        memory_mb_min: va.transform(va.nullish(va.number(vaValueRange(memoryMbRange))), v => v || null),
        storage_mb_max: va.transform(va.nullish(va.number(vaValueRange(storageMbRange))), v => v || null),
        storage_class: va.transform(va.nullish(va.string()), v => v || null)
      })
    )
  )
})

export const PipelineResourcesDialog = (props: {
  pipelineName: string
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
}) => {
  const queryClient = useQueryClient()
  const { mutate: updateConfig } = useMutation(mutationUpdatePipeline(queryClient))
  const onSuccess = (value: va.Input<typeof pipelineConfigSchema>) => {
    invariant(configQuery.data && pipelineQuery.data)
    updateConfig({
      pipelineName: props.pipelineName,
      request: {
        name: pipelineQuery.data.descriptor.name,
        description: pipelineQuery.data.descriptor.description,
        config: {
          ...configQuery.data,
          ...value
        }
      }
    })
    props.setShow(false)
  }
  const onError = () => {}
  const PipelineManagerQuery = usePipelineManagerQuery()
  const configQuery = useQuery({ ...PipelineManagerQuery.pipelineConfig(props.pipelineName), enabled: props.show })
  const pipelineQuery = useQuery({ ...PipelineManagerQuery.pipelineStatus(props.pipelineName), enabled: props.show })
  const disabled = !configQuery.data
  return (
    <Dialog
      fullWidth
      maxWidth='md'
      open={props.show}
      scroll='body'
      onClose={() => props.setShow(false)}
      TransitionComponent={Fade}
      sx={{}}
    >
      <FormContainer
        resolver={valibotResolver(pipelineConfigSchema)}
        values={configQuery.data}
        onSuccess={onSuccess}
        onError={onError}
        shouldUnregister={false}
      >
        <DialogTitle sx={{ px: 6, textAlign: 'center' }}>
          {configQuery.isError ? (
            'Unable to load pipeline config'
          ) : !configQuery.data ? (
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <TwoSeventyRingWithBg fontSize={20} />
              &ensp; Loading pipeline config...
            </Box>
          ) : (
            `${pipelineQuery.data?.descriptor.name ?? 'Pipeline'} runtime resources`
          )}
        </DialogTitle>
        <PipelineResourcesForm disabled={disabled}></PipelineResourcesForm>
      </FormContainer>
    </Dialog>
  )
}

const Label = (props: { children: ReactNode; disabled?: boolean }) => (
  <Typography component='legend' sx={{ pb: 2 }} color={props.disabled ? 'gray' : undefined}>
    {props.children}
  </Typography>
)

export const PipelineResourcesForm = (props: { disabled?: boolean }) => {
  const storageEnabled: boolean = useWatch({ name: 'storage' }) ?? false
  return (
    <>
      <DialogContent sx={{ pb: 0 }}>
        <Grid container columnSpacing={{ xs: 4, sm: 16 }} rowSpacing={4} sx={{ px: 8 }} alignItems={'center'}>
          <Grid item xs={12} sm={4}>
            <Label disabled={props.disabled}>Workers</Label>
            <NumberElement
              name='workers'
              size='small'
              fullWidth
              inputProps={workersRange}
              disabled={props.disabled}
            ></NumberElement>
            <SliderElement
              valueLabelDisplay='off'
              name={'workers'}
              {...workersRange}
              marks={[workersRange.min, workersRange.max / 2, workersRange.max].map(value => ({
                value,
                label: value
              }))}
            />
          </Grid>
          <Grid item xs={12} sm={4}>
            <Label disabled={props.disabled}>Logical CPUs</Label>
            <Box sx={{ display: 'flex', gap: 4, justifyContent: 'space-between' }}>
              <NumberElement
                name='resources.cpu_cores_min'
                optional
                label='Min'
                size='small'
                {...cpuCoresRange}
                placeholder='default'
                disabled={props.disabled}
                InputLabelProps={{
                  shrink: true
                }}
              ></NumberElement>
              <NumberElement
                name='resources.cpu_cores_max'
                optional
                label='Max'
                size='small'
                inputProps={cpuCoresRange}
                placeholder='default'
                disabled={props.disabled}
                InputLabelProps={{
                  shrink: true
                }}
              ></NumberElement>
            </Box>
            <Box sx={{}}>
              <SliderElement
                valueLabelDisplay='off'
                name={['resources.cpu_cores_min', 'resources.cpu_cores_max']}
                {...cpuCoresRange}
                marks={[cpuCoresRange.min, cpuCoresRange.max / 2, cpuCoresRange.max].map(value => ({
                  value,
                  label: value
                }))}
              />
            </Box>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Label disabled={props.disabled}>Memory (GB)</Label>
            <Box sx={{ display: 'flex', gap: 4, justifyContent: 'space-between' }}>
              <NumberElement
                name='resources.memory_mb_min'
                optional
                label='Min'
                size='small'
                {...memoryMbRange}
                multiplier={1000}
                placeholder='default'
                InputLabelProps={{
                  shrink: true
                }}
                disabled={props.disabled}
              ></NumberElement>
              <NumberElement
                name='resources.memory_mb_max'
                optional
                label='Max'
                size='small'
                {...memoryMbRange}
                multiplier={1000}
                placeholder='default'
                InputLabelProps={{
                  shrink: true
                }}
                disabled={props.disabled}
              ></NumberElement>
            </Box>
            <Box sx={{}}>
              <SliderElement
                valueLabelDisplay='off'
                name={['resources.memory_mb_min', 'resources.memory_mb_max']}
                {...memoryMbRange}
                marks={new Array(2)
                  .fill(undefined)
                  .flatMap((_, i) =>
                    new Array(10)
                      .fill(undefined)
                      .map((_, i) => 100 * (i + 1))
                      .map(value => ({
                        value: value * Math.pow(10, i)
                      }))
                  )
                  .concat(
                    new Array(1).fill(undefined).flatMap((_, i) =>
                      new Array(14)
                        .fill(undefined)
                        .map((_, i) => 4000 * (i + 3))
                        .map(value => ({
                          value: value * Math.pow(2, i)
                        }))
                    )
                  )
                  .map(({ value }) => ({
                    value,
                    label: [100, 32000, 64000].find(x => x === value) ? format(value * 1000000, '0bd') : ''
                  }))}
                step={null}
              />
            </Box>
          </Grid>
          <Grid item xs={12} sm={4} alignSelf={'start'}>
            <Box sx={{ display: 'flex', flexDirection: 'column', justifyItems: 'start', height: '100%' }}>
              <Label disabled={props.disabled}>Storage (spill to disk)</Label>
              <SwitchElement
                name='storage'
                label=''
                sx={{ mb: 'auto' }}
                switchProps={{ inputProps: { 'data-testid': 'input-enable-storage' } as any }}
              ></SwitchElement>
            </Box>
          </Grid>
          <Grid item xs={12} sm={4} alignSelf={'start'}>
            <Label disabled={props.disabled || !storageEnabled}>Storage class</Label>
            <TextFieldElement
              name='resources.storage_class'
              label=''
              size='small'
              placeholder='default'
              InputLabelProps={{
                shrink: true
              }}
              disabled={props.disabled || !storageEnabled}
            ></TextFieldElement>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Label disabled={props.disabled || !storageEnabled}>Storage (GB)</Label>
            <Box sx={{ display: 'flex', gap: 4, justifyContent: 'flex-end' }}>
              <NumberElement
                optional
                name='resources.storage_mb_max'
                label='Max'
                size='small'
                {...storageMbRange}
                multiplier={1000}
                placeholder='default'
                InputLabelProps={{
                  shrink: true
                }}
                disabled={props.disabled || !storageEnabled}
              ></NumberElement>
            </Box>
            <Box sx={{}}>
              <SliderElement
                valueLabelDisplay='off'
                name={'resources.storage_mb_max'}
                {...storageMbRange}
                marks={new Array(3)
                  .fill(undefined)
                  .flatMap((_, i) =>
                    new Array(10)
                      .fill(undefined)
                      .map((_, i) => 1000 * (i + 1))
                      .map(value => ({
                        value: value * Math.pow(10, i)
                      }))
                  )
                  .map(({ value }) => ({
                    value,
                    label: [1000, 500000, 1000000].find(x => x === value) ? format(value * 1000000, '0bd') : ''
                  }))}
                step={null}
                disabled={props.disabled || !storageEnabled}
              />
            </Box>
          </Grid>
          <Typography sx={{ px: { xs: 4, sm: 16 } }} color='gray'>
            If storage is enabled it is recommended to configure limit larger than the memory minimum.
          </Typography>
        </Grid>
      </DialogContent>
      <DialogActions sx={{ mt: 4 }}>
        <Typography color='gray' sx={{ pl: 8 }}>
          For running pipelines, changes only take effect upon restart.
        </Typography>
        <Button type={'submit'} disabled={props.disabled} variant='contained' data-testid='button-apply'>
          Apply
        </Button>
      </DialogActions>
    </>
  )
}
