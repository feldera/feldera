import { useLogarithmicSlider } from '$lib/functions/directives/useLogarithmicSlider'
import { valibotRange } from '$lib/functions/valibot'
import { PipelineId } from '$lib/services/manager'
import { mutationUpdatePipeline, PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { format } from 'numerable'
import { Dispatch, SetStateAction } from 'react'
import { FormContainer, TextFieldElement } from 'react-hook-form-mui'
import { SliderElement } from 'src/lib/functions/common/react-hook-form-mui'
import invariant from 'tiny-invariant'
import * as va from 'valibot'
import Icon270RingWithBg from '~icons/svg-spinners/270-ring-with-bg'

import { valibotResolver } from '@hookform/resolvers/valibot'
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Fade,
  FormLabel,
  Stack
} from '@mui/material'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const workersRange = { min: 1, max: 256 }
const cpuCoresRange = { min: 1, max: 256 }
const memoryMbRange = { min: 100, max: 10000 }
const storageMbRange = { min: 0, max: 100000000 }

/** @see '$lib/services/manager/models/ResourceConfig' */
const pipelineConfigSchema = va.object({
  workers: va.optional(va.number(valibotRange(workersRange))),
  resources: va.optional(
    va.object({
      cpu_cores_max: va.nullish(va.number(valibotRange(cpuCoresRange))),
      cpu_cores_min: va.nullish(va.number(valibotRange(cpuCoresRange))),
      memory_mb_max: va.nullish(va.number(valibotRange(memoryMbRange))),
      memory_mb_min: va.nullish(va.number(valibotRange(memoryMbRange))),
      storage_mb_max: va.nullish(va.number(valibotRange(storageMbRange)))
    })
  )
})

export const PipelineResourcesDialog = (props: {
  pipelineId: PipelineId
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
}) => {
  const queryClient = useQueryClient()
  const { mutate: updateConfig } = useMutation(mutationUpdatePipeline(queryClient))
  const onSuccess = (value: va.Output<typeof pipelineConfigSchema>) => {
    invariant(configQuery.data && pipelineQuery.data)
    updateConfig({
      pipelineId: props.pipelineId,
      request: {
        ...pipelineQuery.data.descriptor,
        config: {
          ...configQuery.data,
          ...value
        }
      }
    })
    props.setShow(false)
  }
  const onError = () => {}
  const configQuery = useQuery({ ...PipelineManagerQuery.pipelineConfig(props.pipelineId), enabled: props.show })
  const pipelineQuery = useQuery({ ...PipelineManagerQuery.pipelineStatus(props.pipelineId), enabled: props.show })
  const disabled = !configQuery.data
  return (
    <Dialog
      fullWidth
      maxWidth='xs'
      open={props.show}
      scroll='body'
      onClose={() => props.setShow(false)}
      TransitionComponent={Fade}
      sx={{}}
    >
      <FormContainer
        resolver={valibotResolver(pipelineConfigSchema)}
        values={configQuery.data}
        defaultValues={{ resources: { cpu_cores_min: 2, cpu_cores_max: 6 } }}
        onSuccess={onSuccess}
        onError={onError}
        shouldUnregister={false}
      >
        <DialogTitle sx={{ px: 6 }}>
          {configQuery.isError ? (
            'Unable to load pipeline config'
          ) : !configQuery.data ? (
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <Icon270RingWithBg fontSize={20} />
              &ensp; Loading pipeline config...
            </Box>
          ) : (
            'Pipeline runtime resources'
          )}
          <DialogContentText>{pipelineQuery.data?.descriptor.name ?? <>&nbsp;</>}</DialogContentText>
        </DialogTitle>
        <DialogContent sx={{ pb: 0 }}>
          <Stack spacing={2} sx={{ px: 8 }}>
            <Box sx={{}}>
              <FormLabel component='legend'>Workers</FormLabel>
              <TextFieldElement
                name='workers'
                type={'number'}
                size='small'
                placeholder='none'
                inputProps={workersRange}
                disabled={disabled}
              ></TextFieldElement>
              <SliderElement
                sx={{}}
                step={null}
                valueLabelDisplay='off'
                name={'workers'}
                {...useLogarithmicSlider({
                  marks: new Array(2).fill(undefined).flatMap((_, i) =>
                    new Array(16)
                      .fill(undefined)
                      .map((_, i) => i + 1)
                      .map(value => ({
                        value: value * Math.pow(16, i),
                        label: Number.isInteger(Math.log2(value) / 2) ? value * Math.pow(16, i) : ''
                      }))
                  ),
                  max: 100,
                  maxLog: workersRange.max
                })}
              />
            </Box>
            <Box>
              <FormLabel component='legend'>Logical CPUs</FormLabel>
              <Box sx={{ display: 'flex', gap: 4, justifyContent: 'space-between', pt: 2 }}>
                <TextFieldElement
                  name='resources.cpu_cores_min'
                  label='Min'
                  type={'number'}
                  size='small'
                  inputProps={cpuCoresRange}
                  placeholder='none'
                  InputLabelProps={{
                    shrink: true
                  }}
                  disabled={disabled}
                ></TextFieldElement>
                <TextFieldElement
                  name='resources.cpu_cores_max'
                  label='Max'
                  type={'number'}
                  size='small'
                  inputProps={cpuCoresRange}
                  placeholder='none'
                  InputLabelProps={{
                    shrink: true
                  }}
                  disabled={disabled}
                ></TextFieldElement>
              </Box>
              <Box sx={{}}>
                <SliderElement
                  sx={{}}
                  step={null}
                  valueLabelDisplay='off'
                  name={['resources.cpu_cores_min', 'resources.cpu_cores_max']}
                  {...useLogarithmicSlider({
                    marks: new Array(2).fill(undefined).flatMap((_, i) =>
                      new Array(16)
                        .fill(undefined)
                        .map((_, i) => i + 1)
                        .map(value => ({
                          value: value * Math.pow(16, i),
                          label: Number.isInteger(Math.log2(value) / 2) ? value * Math.pow(16, i) : ''
                        }))
                    ),
                    maxLog: 256
                  })}
                />
              </Box>
              <Box sx={{ display: 'flex', width: '100%', gap: 2, pt: 2 }}></Box>
            </Box>
            <Box sx={{}}>
              <FormLabel component='legend'>Memory MB</FormLabel>
              <Box sx={{ display: 'flex', gap: 4, justifyContent: 'space-between', pt: 2 }}>
                <TextFieldElement
                  name='resources.memory_mb_min'
                  label='Min'
                  type={'number'}
                  size='small'
                  inputProps={{ ...memoryMbRange, step: 100 }}
                  placeholder='none'
                  InputLabelProps={{
                    shrink: true
                  }}
                  disabled={disabled}
                ></TextFieldElement>
                <TextFieldElement
                  name='resources.memory_mb_max'
                  label='Max'
                  type={'number'}
                  size='small'
                  inputProps={{ ...memoryMbRange, step: 100 }}
                  placeholder='none'
                  InputLabelProps={{
                    shrink: true
                  }}
                  disabled={disabled}
                ></TextFieldElement>
              </Box>
              <Box sx={{ display: 'flex' }}>
                <SliderElement
                  step={null}
                  valueLabelDisplay='off'
                  name={['resources.memory_mb_min', 'resources.memory_mb_max']}
                  {...useLogarithmicSlider({
                    fromValue: v => v / 100,
                    toValue: v => v * 100,
                    marks: new Array(2).fill(undefined).flatMap((_, i) =>
                      new Array(10)
                        .fill(undefined)
                        .map((_, i) => 100 * (i + 1))
                        .map(value => ({
                          value: value * Math.pow(10, i),
                          label: Number.isInteger(Math.log10(value * Math.pow(10, i)))
                            ? (value => format(value * 1000000, '0.00bd'))(value * Math.pow(10, i))
                            : ''
                        }))
                        .concat({ value: 1500 * Math.pow(10, i), label: '' })
                    ),
                    max: 100,
                    maxLog: 100
                  })}
                />
              </Box>
            </Box>
            <Box sx={{}}>
              <FormLabel component='legend'>Storage MB</FormLabel>

              <Box sx={{ display: 'flex', gap: 4, justifyContent: 'flex-end', pt: 2 }}>
                <TextFieldElement
                  name='resources.storage_mb_max'
                  label='Max'
                  type={'number'}
                  size='small'
                  inputProps={{ ...storageMbRange }}
                  placeholder='none'
                  InputLabelProps={{
                    shrink: true
                  }}
                  disabled={disabled}
                ></TextFieldElement>
              </Box>
              <Box sx={{}}>
                <SliderElement
                  sx={{}}
                  step={null}
                  valueLabelDisplay='off'
                  name={'resources.storage_mb_max'}
                  {...useLogarithmicSlider({
                    fromValue: v => v / 100,
                    toValue: v => v * 100,
                    marks: new Array(5).fill(undefined).flatMap((_, i) =>
                      new Array(10)
                        .fill(undefined)
                        .map((_, i) => 100 * (i + 1))
                        .map(value => ({
                          value: value * Math.pow(10, i),
                          label: Number.isInteger(Math.log10(value * Math.pow(10, i)) / 2)
                            ? (value => format(value * 1000000, '0.00bd'))(value * Math.pow(10, i))
                            : ''
                        }))
                    ),
                    max: 100,
                    maxLog: 100000
                  })}
                />
              </Box>
            </Box>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button type={'submit'} disabled={disabled}>
            Apply
          </Button>
        </DialogActions>
      </FormContainer>
    </Dialog>
  )
}
