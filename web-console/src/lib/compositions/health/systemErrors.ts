import { base } from '$app/paths'
import { defaultGithubReportSections, type ReportDetails } from '$lib/services/githubReport'
import type { ErrorResponse } from '$lib/services/manager'
import {
  getPipeline,
  type ExtendedPipeline,
  type Pipeline,
  type PipelineThumb,
  type SqlCompilerMessage
} from '$lib/services/pipelineManager'
import type { ControllerStatus } from '$lib/types/pipelineManager'

import JSONbig from 'true-json-bigint'
import { match, P } from 'ts-pattern'

export type SystemError<T = any, Report = ReportDetails> = Error & {
  message: string
  cause: {
    entityName: string
    source: string
    report: Report
    tag: string
    body: T
    warning?: boolean
  }
}

const limitMessage = (text: string | null | undefined, max: number, prefix: string) =>
  ((t) => (t.length > max ? prefix : '') + t.slice(Math.max(0, t.length - max)))(text || '')

export const extractPipelineErrors = (pipeline: ExtendedPipeline) => {
  if (!(typeof pipeline.status === 'object' && 'PipelineError' in pipeline.status)) {
    return []
  }
  const error = pipeline.status.PipelineError
  return [
    (() => ({
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
          '6-extra': (() => {
            const programCode =
              'SQL:\n```\n' +
              limitMessage(pipeline.programCode, 6600, '\n...Beginning of the code...') +
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

export const programErrorReport = (pipeline: Pipeline) => (pipelineName: string, message: string) =>
  ({
    ...defaultGithubReportSections,
    name: 'Report: program compilation error',
    '1-description':
      '```\n' + limitMessage(message, 1000, '\n...Beginning of the error...') + '\n```',
    '6-extra':
      'SQL:\n```\n' +
      limitMessage(pipeline.programCode, 7000, '\n...Beginning of the code...') +
      '\n```'
  }) as ReportDetails

const fetchedProgramErrorReport = async (pipelineName: string, message: string) => {
  const pipeline = await getPipeline(pipelineName)
  return programErrorReport(pipeline)(pipelineName, message)
}

export const showSqlCompilerMessage = (e: SqlCompilerMessage) =>
  `${e.error_type ? e.error_type + ':\n' : ''}${e.message}${e.snippet ? '\n' + e.snippet : ''}`

export const extractProgramError =
  <Report>(getReport: (pipelineName: string, message: string) => Report) =>
  (pipeline: {
    name: string
    status:
      | { RustError: string }
      | { SystemError: string }
      | { SqlError: SqlCompilerMessage[] }
      | string
      | { PipelineError: ErrorResponse }
  }) => {
    const source = `${base}/pipelines/${encodeURI(pipeline.name)}/`
    const result = match(pipeline.status)
      .returnType<SystemError<any, Report>[]>()
      .with({ RustError: P.any }, (e) => [
        (() => ({
          name: `Error compiling ${pipeline.name}`,
          message:
            'Program compilation error. See details below:\n' +
            e.RustError,
          cause: {
            entityName: pipeline.name,
            tag: 'programError',
            source,
            report: getReport(pipeline.name, e.RustError),
            body: e.RustError
          }
        }))()
      ])
      .with(
        {
          SystemError: P.any
        },
        (e) => [
          (() => ({
            name: `Error compiling ${pipeline.name}`,
            message: e.SystemError,
            cause: {
              entityName: pipeline.name,
              tag: 'programError',
              source,
              report: getReport(pipeline.name, e.SystemError),
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
          es.SqlError.map((e) => ({
            name: `Error in SQL code of ${pipeline.name}`,
            message: showSqlCompilerMessage(e),
            cause: {
              entityName: pipeline.name,
              tag: 'programError',
              source:
                source +
                '#:' +
                e.start_line_number +
                (e.start_column > 1 ? ':' + e.start_column.toString() : ''),
              report: getReport(pipeline.name, e.message),
              body: e,
              warning: e.warning
            }
          }))
      )
      .otherwise(() => [])
    return result
  }

export const extractPipelineXgressErrors = ({
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
  return stats.inputs
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
}
