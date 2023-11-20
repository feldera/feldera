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
const storageMbRange = { min: 100, max: 10000000 }

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
                valueLabelDisplay='off'
                name={'workers'}
                {...workersRange}
                marks={[workersRange.min, 128, workersRange.max].map(value => ({
                  value,
                  label: value
                }))}
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
                  valueLabelDisplay='off'
                  name={['resources.cpu_cores_min', 'resources.cpu_cores_max']}
                  {...cpuCoresRange}
                  marks={[cpuCoresRange.min, 128, cpuCoresRange.max].map(value => ({
                    value,
                    label: value
                  }))}
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
                  valueLabelDisplay='off'
                  name={['resources.memory_mb_min', 'resources.memory_mb_max']}
                  {...memoryMbRange}
                  marks={[memoryMbRange.min, 5000, memoryMbRange.max].map(value => ({
                    value,
                    label: format(value * 1000000, '0.00bd')
                  }))}
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
                  valueLabelDisplay='off'
                  name={'resources.storage_mb_max'}
                  {...storageMbRange}
                  marks={[storageMbRange.min, 5000000, storageMbRange.max].map(value => ({
                    value,
                    label: format(value * 1000000, '0.00bd')
                  }))}
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
