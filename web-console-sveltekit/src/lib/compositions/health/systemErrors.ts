import { base } from '$app/paths'
import { nonNull } from '$lib/functions/common/function'
import { handled } from '$lib/functions/request'
import { defaultGithubReportSections, type ReportDetails } from '$lib/services/githubReport'
import {
  getPipeline,
  getPipelines,
  getPipelineStats,
  type PipelineStatus,
  type PipelineThumb
} from '$lib/services/pipelineManager'
import type { ControllerStatus } from '$lib/types/pipelineManager'
import { asyncDerived, asyncReadable, derived, get, type Readable } from '@square/svelte-store'
import { onMount } from 'svelte'

import JSONbig from 'true-json-bigint'
import { match, P } from 'ts-pattern'

export type SystemError<T = any> = Error & {
  message: string
  cause: {
    entityName: string
    source: string
    report: ReportDetails
    tag: string
    body: T
  }
}

const limitMessage = (text: string | null | undefined, max: number, prefix: string) =>
  ((t) => (t.length > max ? prefix : '') + t.slice(Math.max(0, t.length - max)))(text || '')

const extractPipelineErrors = (pipeline: PipelineThumb) => {
  if (!(typeof pipeline.status === 'object' && 'PipelineError' in pipeline.status)) {
    return []
  }
  const error = pipeline.status.PipelineError
  return [
    (async () => ({
      name: `Error running pipeline ${pipeline.name}`,
      message: error.message,
      cause: {
        entityName: pipeline.name,
        tag: 'pipelineError',
        source: `${base}/pipelines/${pipeline.name}/`,
        report: {
          ...defaultGithubReportSections,
          name: 'Report: pipeline execution error',
          '1-description':
            '```\n' + limitMessage(error.message, 1000, '\n...Beginning of the error...') + '\n```',
          '6-extra': await (async () => {
            const fullPipeline = await getPipeline(pipeline.name)
            const programCode =
              'SQL:\n```\n' +
              limitMessage(fullPipeline.programCode, 6600, '\n...Beginning of the code...') +
              '\n```'
            const pipelineConfig =
              'Pipelince config:\n```\n' +
              JSONbig.stringify(pipeline.runtimeConfig, undefined, '\t') +
              '\n```\n'
            return pipelineConfig + programCode
          })()
        } as ReportDetails,
        body: error.details
      }
    }))()
  ]
}

const programErrorReport = async (pipeline: { name: string }, message: string) =>
  ({
    ...defaultGithubReportSections,
    name: 'Report: program compilation error',
    '1-description':
      '```\n' + limitMessage(message, 1000, '\n...Beginning of the error...') + '\n```',
    '6-extra': await getPipeline(pipeline.name).then(
      (p) =>
        'SQL:\n```\n' + limitMessage(p.programCode, 7000, '\n...Beginning of the code...') + '\n```'
    )
  }) as ReportDetails

const extractProgramError = (pipeline: { name: string; status: PipelineStatus }) => {
  const source = `${base}/pipelines/${encodeURI(pipeline.name)}/`
  const result = match(pipeline.status)
    .returnType<Promise<SystemError>[]>()
    .with({ RustError: P.any }, (e) => [
      (async () => ({
        name: `Error compiling ${pipeline.name}`,
        message: 'Compilation error occurred when compiling the program - see the details below:\n' + e.RustError,
        cause: {
          entityName: pipeline.name,
          tag: 'programError',
          source,
          report: await programErrorReport(pipeline, e.RustError),
          body: e.RustError
        }
      }))()
    ])
    .with(
      {
        SystemError: P.any
      },
      (e) => [
        (async () => ({
          name: `Error compiling ${pipeline.name}`,
          message: e.SystemError,
          cause: {
            entityName: pipeline.name,
            tag: 'programError',
            source,
            report: await programErrorReport(pipeline, e.SystemError),
            body: e.SystemError
          }
        }))()
      ]
    )
    .with(
      {
        SqlError: P.any
      },
      (es) =>
        es.SqlError.map(async (e) => ({
          name: `Error in SQL code of ${pipeline.name}`,
          message: e.message, // 'Go to the source or expand to see the error',
          cause: {
            entityName: pipeline.name,
            tag: 'programError',
            source:
              source +
              '#:' +
              e.startLineNumber +
              (e.startColumn > 1 ? ':' + e.startColumn.toString() : ''),
            report: await programErrorReport(pipeline, e.message),
            body: e
          }
        }))
    )
    .with(
      'Shutdown',
      'Initializing',
      'Paused',
      'Running',
      'ShuttingDown',
      'Compiling sql',
      'Queued',
      'Compiling bin',
      'Starting up',
      { PipelineError: P.any },
      () => []
    )
    .exhaustive()
  return result
}

const extractPipelineXgressErrors = ({
  pipelineName,
  status
}: {
  pipelineName: string
  status: Pick<ControllerStatus, 'inputs' | 'outputs'> | null | 'not running'
}): SystemError[] => {
  const stats = status == null || status === 'not running' ? { inputs: [], outputs: [] } : status
  const source = `${base}/pipelines/${pipelineName}/`
  const stringifyConfig = (config: any) =>
    `Connector config:\n\`\`\`\n${JSONbig.stringify(config, undefined, '\t')}\n\`\`\`\n`
  const z = stats.inputs
    .flatMap((input) => [
      ...(input.metrics.num_parse_errors
        ? [
            {
              name: `${input.metrics.num_parse_errors} connector parse errors in ${pipelineName}`,
              message: `${input.metrics.num_parse_errors} parse errors in ${input.config.transport.name} connector ${input.endpoint_name} of ${pipelineName}`,
              cause: {
                entityName: pipelineName,
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
          ]
        : []),
      ...(input.metrics.num_transport_errors
        ? [
            {
              name: `${input.metrics.num_transport_errors} connector transport errors in ${pipelineName}`,
              message: `${input.metrics.num_transport_errors} transport errors in ${input.config.transport.name} connector ${input.endpoint_name} of ${pipelineName}`,
              cause: {
                entityName: pipelineName,
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
          ]
        : [])
    ])
    .concat(
      stats.outputs.flatMap((output) => [
        ...(output.metrics.num_encode_errors
          ? [
              {
                name: `${output.metrics.num_encode_errors} connector encode errors in ${pipelineName}`,
                message: `${output.metrics.num_encode_errors} encode errors in ${output.config.transport.name} connector ${output.endpoint_name} of ${pipelineName}`,
                cause: {
                  entityName: pipelineName,
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
            ]
          : []),
        ...(output.metrics.num_transport_errors
          ? [
              {
                name: `${output.metrics.num_transport_errors} connector transport errors in ${pipelineName}`,
                message: `${output.metrics.num_transport_errors} transport errors in ${output.config.transport.name} connector ${output.endpoint_name} of ${pipelineName}`,
                cause: {
                  entityName: pipelineName,
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
            ]
          : [])
      ])
    )
  return z
}

const referencePipelines = asyncReadable([], () => getPipelines(), { reloadable: true })
const pipelinesErrors = asyncDerived(
  referencePipelines,
  (ps) => Promise.all<SystemError>(ps.flatMap(extractPipelineErrors)),
  { initial: [] }
)
const programsErrors = asyncDerived(
  referencePipelines,
  (ps) => {
    return Promise.all<SystemError>(ps.flatMap(extractProgramError)).then(
      (errors) => errors,
      () => {
        return [] as SystemError[]
      }
    )
  },
  { reloadable: true, initial: [] }
)
const pipelineXgressErrors = asyncDerived(
  referencePipelines,
  (ps) =>
    Promise.all(ps.map((p) => getPipelineStats(p.name))).then((ss) => {
      return ss.flatMap(extractPipelineXgressErrors)
    }),
  { initial: [] }
)
const systemErrors = derived(
  [programsErrors, pipelinesErrors, pipelineXgressErrors],
  ([a, b, c]) => {
    return ([] as SystemError[]).concat(a, b, c)
  }
)

export const useSqlErrors = (pipelineName?: Readable<string>) => {
  if (pipelineName) {
    return derived([pipelineName, programsErrors], ([pipelineName, errors]) =>
      errors.filter((error) => error.cause.entityName === pipelineName)
    )
  }
  return programsErrors
}

export const useSystemErrors = () => {
  onMount(() => {
    let interval = setInterval(() => {
      systemErrors.reload?.()
    }, 2000)
    return () => {
      clearInterval(interval)
    }
  })
  return systemErrors
}
