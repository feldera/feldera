import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { nonNull } from '$lib/functions/common/function'
import { defaultGithubReportSections, ReportDetails } from '$lib/services/feedback'
import { ProgramDescr, ProgramsService } from '$lib/services/manager'
import {
  pipelineManagerAggregateQuery,
  SystemError,
  updateSystemErrorsCache
} from '$lib/services/pipelineManagerAggregateQuery'
import { ControllerStatus, Pipeline, PipelineStatus } from '$lib/types/pipeline'
import JSONbig from 'true-json-bigint'
import { match, P } from 'ts-pattern'

import { useQueries, useQuery, useQueryClient } from '@tanstack/react-query'

const limitMessage = (text: string | null | undefined, max: number, prefix: string) =>
  (t => (t.length > max ? prefix : '') + t.slice(Math.max(0, t.length - max)))(text || '')

const extractPipelineError = async (pipeline: Pipeline) => {
  const error = pipeline.state.error
  if (!error) {
    return undefined
  }
  return {
    name: `Error running pipeline ${pipeline.descriptor.name}`,
    message: error.message,
    cause: {
      tag: 'pipelineError',
      source: `/streaming/builder/?pipeline_name=${pipeline.descriptor.name}`,
      report: {
        ...defaultGithubReportSections,
        name: 'Report: pipeline execution error',
        '1-description': '```\n' + limitMessage(error.message, 1000, '\n...Beginning of the error...') + '\n```',
        '6-extra': await (async () => {
          const programName = pipeline.descriptor.program_name
          const programCode = !programName
            ? ''
            : await ProgramsService.getProgram(programName, true).then(
                program => 'SQL:\n```\n' + limitMessage(program.code, 6600, '\n...Beginning of the code...') + '\n```'
              )
          const pipelineConfig =
            'Pipelince config:\n```\n' + JSONbig.stringify(pipeline.descriptor.config, undefined, '\t') + '\n```\n'
          return pipelineConfig + programCode
        })()
      } as ReportDetails,
      body: error.details
    }
  }
}

const usePipelineErrors = () => {
  const queryClient = useQueryClient()
  const pipelineManagerQuery = usePipelineManagerQuery()
  const { data } = useQuery({
    ...pipelineManagerQuery.pipelines(),
    refetchInterval: 2000,
    refetchOnWindowFocus: false,
    select(data) {
      Promise.all(data.map(extractPipelineError)).then(errors => {
        updateSystemErrorsCache(queryClient, errors.filter(nonNull))
      })
      return data
    }
  })
  return data
}

const programErrorReport = async (programName: string, message: string) =>
  ({
    ...defaultGithubReportSections,
    name: 'Report: program compilation error',
    '1-description': '```\n' + limitMessage(message, 1000, '\n...Beginning of the error...') + '\n```',
    '6-extra': await ProgramsService.getProgram(programName, true).then(
      p => 'SQL:\n```\n' + limitMessage(p.code, 7000, '\n...Beginning of the code...') + '\n```'
    )
  }) as ReportDetails

const extractProgramError = (program: ProgramDescr) => {
  const source = `/analytics/editor/?program_name=${program.name}`
  return match(program.status)
    .returnType<(Promise<SystemError> | undefined)[]>()
    .with({ RustError: P.any }, e => [
      (async () => ({
        name: `Error compiling ${program.name}`,
        message: 'Compilation error occurred when compiling the program - see the details below',
        cause: {
          tag: 'programError',
          source,
          report: await programErrorReport(program.name, e.RustError),
          body: e.RustError
        }
      }))()
    ])
    .with(
      {
        SystemError: P.any
      },
      e => [
        (async () => ({
          name: `Error compiling ${program.name}`,
          message: e.SystemError,
          cause: {
            tag: 'programError',
            source,
            report: await programErrorReport(program.name, e.SystemError),
            body: e.SystemError
          }
        }))()
      ]
    )
    .with(
      {
        SqlError: P.any
      },
      es =>
        es.SqlError.map(async e => ({
          name: `Error in SQL code of ${program.name}`,
          message: 'Go to the source or expand to see the error',
          cause: {
            tag: 'programError',
            source,
            report: await programErrorReport(program.name, e.message),
            body: e.message
          }
        }))
    )
    .with('Success', 'Pending', 'CompilingSql', 'CompilingRust', () => [undefined])
    .exhaustive()
}

const useProgramErrors = () => {
  const queryClient = useQueryClient()
  const pipelineManagerQuery = usePipelineManagerQuery()
  useQuery({
    ...pipelineManagerQuery.programs(),
    refetchInterval: 2000,
    refetchOnWindowFocus: false,
    async select(data) {
      const errors = await Promise.all(data.flatMap(extractProgramError).filter(nonNull))
      updateSystemErrorsCache(queryClient, errors)
    }
  })
}

const extractPipelineXgressErrors = ({
  pipelineName,
  status
}: {
  pipelineName: string
  status: ControllerStatus
}): SystemError[] => {
  const source = `/streaming/builder/?pipeline_name=${pipelineName}`
  const stringifyConfig = (config: any) =>
    `Connector config:\n\`\`\`\n${JSONbig.stringify(config, undefined, '\t')}\n\`\`\`\n`
  return status.inputs
    .flatMap(input =>
      [
        input.metrics.num_parse_errors
          ? {
              name: `${input.metrics.num_parse_errors} connector parse errors in ${pipelineName}`,
              message: `${input.metrics.num_parse_errors} parse errors in ${input.config.transport.name} connector ${input.endpoint_name} of ${pipelineName}`,
              cause: {
                tag: 'xgressError',
                source,
                report: {
                  ...defaultGithubReportSections,
                  name: `Report: ${input.config.transport.name} connector parse errors`,
                  '6-extra': stringifyConfig(input.config)
                } as ReportDetails,
                body: ''
              }
            }
          : undefined,
        input.metrics.num_transport_errors
          ? {
              name: `${input.metrics.num_transport_errors} connector transport errors in ${pipelineName}`,
              message: `${input.metrics.num_transport_errors} transport errors in ${input.config.transport.name} connector ${input.endpoint_name} of ${pipelineName}`,
              cause: {
                tag: 'xgressError',
                source,
                report: {
                  ...defaultGithubReportSections,
                  name: `Report: ${input.config.transport.name} connector transport errors`,
                  '6-extra': stringifyConfig(input.config)
                } as ReportDetails,
                body: ''
              }
            }
          : undefined
      ].filter(nonNull)
    )
    .concat(
      status.outputs.flatMap(output =>
        [
          output.metrics.num_encode_errors
            ? {
                name: `${output.metrics.num_encode_errors} connector encode errors in ${pipelineName}`,
                message: `${output.metrics.num_encode_errors} encode errors in ${output.config.transport.name} connector ${output.endpoint_name} of ${pipelineName}`,
                cause: {
                  tag: 'xgressError',
                  source,
                  report: {
                    ...defaultGithubReportSections,
                    name: `Report: ${output.config.transport.name} connector encode errors`,
                    '6-extra': stringifyConfig(output.config)
                  } as ReportDetails,
                  body: ''
                }
              }
            : undefined,
          output.metrics.num_transport_errors
            ? {
                name: `${output.metrics.num_transport_errors} connector transport errors in ${pipelineName}`,
                message: `${output.metrics.num_transport_errors} transport errors in ${output.config.transport.name} connector ${output.endpoint_name} of ${pipelineName}`,
                cause: {
                  tag: 'xgressError',
                  source,
                  report: {
                    ...defaultGithubReportSections,
                    name: `Report: ${output.config.transport.name} connector transport errors`,
                    '6-extra': stringifyConfig(output.config)
                  } as ReportDetails,
                  body: ''
                }
              }
            : undefined
        ].filter(nonNull)
      )
    )
}

const usePipelineXgressErrors = (pipelines: Pipeline[] | undefined) => {
  const queryClient = useQueryClient()
  const pipelineManagerQuery = usePipelineManagerQuery()
  const runningPipelines = pipelines?.filter(pipeline => pipeline.state.current_status === PipelineStatus.RUNNING)
  const select = (data: { pipelineName: string; status: ControllerStatus | null }) => {
    if (!data.status) {
      return
    }
    const errors = extractPipelineXgressErrors({ pipelineName: data.pipelineName, status: data.status })
    updateSystemErrorsCache(queryClient, errors)
  }
  useQueries({
    queries: !runningPipelines
      ? []
      : runningPipelines.map(pipeline =>
          (query => ({ ...query, select, refetchInterval: 2000 }))(
            pipelineManagerQuery.pipelineStats(pipeline.descriptor.name)
          )
        )
  })
}

export const useSystemErrors = () => {
  useProgramErrors()
  const pipelines = usePipelineErrors()
  usePipelineXgressErrors(pipelines)
  const { data: systemErrors } = useQuery({
    ...pipelineManagerAggregateQuery.systemErrors(),
    refetchInterval: 2000,
    refetchOnWindowFocus: false
  })
  return {
    systemErrors: systemErrors ?? []
  }
}
