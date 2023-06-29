import { PipelineDescr, PipelineRevision, ProgramCodeResponse } from 'src/types/manager'
import CustomChip from 'src/@core/components/mui/chip'
import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import diff from 'fast-diff'
import Badge from '@mui/material/Badge'

export interface Props {
  pipeline: PipelineDescr
}

export const PipelineRevisionDiffViewer = () => {
  return
}

export const PipelineRevisionStatusChip = (props: Props) => {
  const pipeline = props.pipeline
  const [diffCount, setDiffCount] = useState<number | undefined>(undefined)
  const [label, setLabel] = useState<string | undefined>(undefined)
  const [show, setShow] = useState<boolean>(false)

  const curPipelineConfigQuery = useQuery<string>(['pipelineConfig', { pipeline_id: pipeline.pipeline_id }])
  const curProgramQuery = useQuery<ProgramCodeResponse>(['programCode', { program_id: pipeline.program_id }])
  const pipelineRevisionQuery = useQuery<PipelineRevision>([
    'pipelineLastRevision',
    { pipeline_id: pipeline.pipeline_id }
  ])
  useEffect(() => {
    if (
      !pipelineRevisionQuery.isLoading &&
      !pipelineRevisionQuery.isError &&
      !curPipelineConfigQuery.isLoading &&
      !curPipelineConfigQuery.isError &&
      !curProgramQuery.isLoading &&
      !curProgramQuery.isError &&
      curProgramQuery.data
    ) {
      if (pipelineRevisionQuery.data != null) {
        const configDiffResult = diff(pipelineRevisionQuery.data.config, curPipelineConfigQuery.data)
        const programDiffResult = diff(pipelineRevisionQuery.data.code, curProgramQuery.data.code)
        setDiffCount(configDiffResult.length + programDiffResult.length)
        if (configDiffResult.length > 1 || programDiffResult.length > 1) {
          setLabel('Modified')
          setShow(true)
        }
      } else {
        setShow(false)
      }
    }
  }, [
    pipelineRevisionQuery.isLoading,
    pipelineRevisionQuery.isError,
    pipelineRevisionQuery.data,
    curPipelineConfigQuery.isLoading,
    curPipelineConfigQuery.isError,
    curPipelineConfigQuery.data,
    curProgramQuery.isLoading,
    curProgramQuery.isError,
    curProgramQuery.data,
    setDiffCount,
    setLabel
  ])

  return show ? (
    <Badge badgeContent={diffCount} color='success'>
      <CustomChip rounded size='small' skin='light' label={label} />
    </Badge>
  ) : (
    <></>
  )
}
