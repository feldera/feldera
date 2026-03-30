import { count, groupBy } from '$lib/functions/common/array'
import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
import { resolve } from '$lib/functions/svelte'
import { defaultGithubReportSections, type ReportDetails } from '$lib/services/githubReport'
import type {
  CompilerOutput,
  ExtendedPipeline,
  Pipeline,
  SqlCompilerMessage
} from '$lib/services/pipelineManager'

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

export const numConnectorsWithProblems = (metrics: PipelineMetrics) => {
  return (
    count(
      metrics.inputs,
      (i) =>
        (i.metrics.num_parse_errors ?? 0) > 0 ||
        (i.metrics.num_transport_errors ?? 0) > 0 ||
        i.health?.status === 'Unhealthy'
    ) +
    count(
      metrics.outputs,
      (o) =>
        (o.metrics.num_encode_errors ?? 0) > 0 ||
        (o.metrics.num_transport_errors ?? 0) > 0 ||
        o.health?.status === 'Unhealthy'
    )
  )
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

// const fetchedProgramErrorReport = async (pipelineName: string, message: string) => {
//   const pipeline = await getExtendedPipeline(pipelineName)
//   return programErrorReport(pipeline)(pipelineName, message)
// }

export const showSqlCompilerMessage = (e: SqlCompilerMessage) =>
  `${e.warning ? 'warning' : 'error'}: ${e.error_type ? e.error_type + '\n' : ''}${e.message}${e.snippet ? '\n' + e.snippet : ''}`

export const extractInternalCompilationError = <Report>(
  stderr: string,
  pipelineName: string,
  source: string,
  getReport: (pipelineName: string, message: string) => Report
): SystemError<any, Report> | null => {
  const isInternalError = /main\.rs/.test(stderr)
  if (!isInternalError) {
    return null
  }
  return {
    name: `Error compiling ${pipelineName}`,
    message: stderr,
    cause: {
      entityName: pipelineName,
      tag: 'programError',
      source: source + '#program.sql',
      report: getReport(pipelineName, stderr),
      body: {
        startLineNumber: 0,
        endLineNumber: 9999,
        startColumn: 0,
        endColumn: 9999,
        message: stderr.match(/([\S\s]+?)\n\n/)?.[1] ?? 'Unknown internal compilation error' // Return first stderr paragraph as error body
      }
    }
  }
}

export const extractRustCompilerError = <Report>(
  pipelineName: string,
  source: string,
  getReport: (pipelineName: string, message: string) => Report
) => {
  const matchFileError = (
    stderr: string,
    warning: boolean,
    fileName: string,
    fileRegex: RegExp,
    lineOffset: number
  ) => {
    const match = stderr.match(fileRegex)
    if (!match) {
      return undefined
    }
    const startLineNumber = parseInt(match[1]) + lineOffset
    const startColumn = parseInt(match[2])
    return {
      name: `${warning ? 'Warning in' : 'Error compiling'} ${pipelineName}`,
      message: stderr, // 'Program compilation error. See details below:\n' + stderr
      cause: {
        entityName: pipelineName,
        tag: 'programError',
        source:
          source +
          `#${fileName}:` +
          startLineNumber +
          (startColumn > 0 ? ':' + startColumn.toString() : ''),
        report: getReport(pipelineName, stderr),
        body: {
          startLineNumber: startLineNumber,
          endLineNumber: startLineNumber,
          startColumn: startColumn,
          endColumn: startColumn + 10,
          message: stderr
        },
        warning
      }
    }
  }
  return (stderr: string): SystemError<any, Report> => {
    const warning = /^warning:/.test(stderr)
    let err: SystemError<any, Report> | undefined
    err = matchFileError(stderr, warning, 'udf.toml', /\/Cargo\.toml:(\d+):(\d+)/, -10)
    if (err) {
      return err
    }
    err = matchFileError(stderr, warning, 'udf.rs', /\/udf\.rs:(\d+):(\d+)/, 0)
    if (err) {
      return err
    }
    err = matchFileError(stderr, warning, 'stubs.rs', /\/stubs\.rs:(\d+):(\d+)/, 0)
    if (err) {
      return err
    }

    return {
      name: `${warning ? 'Warning in' : 'Error compiling'} ${pipelineName}`,
      message: stderr,
      cause: {
        entityName: pipelineName,
        tag: 'unrecognizedProgramError',
        source: source + '#program.sql',
        report: getReport(pipelineName, stderr),
        body: warning
          ? stderr
          : {
              startLineNumber: 0,
              endLineNumber: 9999,
              startColumn: 0,
              endColumn: 9999,
              message: stderr
            },
        warning
      }
    }
  }
}

const ignoredRustErrors = ['warning: patch for the non root package will be ignored']

/**
 * @returns Errors associated with source files
 */
export const extractProgramErrors =
  <Report>(getReport: (pipelineName: string, message: string) => Report) =>
  (pipeline: Pick<ExtendedPipeline, 'name' | 'status' | 'compilerOutput'>) => {
    const source = resolve(`/pipelines/${encodeURI(pipeline.name)}/`)
    const result: SystemError<any, Report>[] = []
    if (pipeline.compilerOutput.sql) {
      result.push.apply(
        result,
        ((messages) =>
          messages.map((e) => ({
            name: `Error in SQL code of ${pipeline.name}`,
            message: showSqlCompilerMessage(e),
            cause: {
              entityName: pipeline.name,
              tag: 'programError',
              source:
                source +
                '#program.sql:' +
                e.start_line_number +
                (e.start_column > 1 ? ':' + e.start_column.toString() : ''),
              report: getReport(pipeline.name, e.message),
              body: e,
              warning: e.warning
            }
          })))(pipeline.compilerOutput.sql.messages)
      )
    }
    if (pipeline.compilerOutput.rust) {
      result.push.apply(
        result,
        ((stderr) => {
          // $(?![\r\n]) - RegEx for the end of a string with multiline flag (/gm)
          const rustCompilerErrorRegex =
            /^((warning:(?! `)|error(\[[\w]+\])?:)([\s\S])+?)\n(\n|(?=error|warning))/gm
          const rustInternalCompilerError = extractInternalCompilationError(
            stderr,
            pipeline.name,
            source,
            getReport
          )
          if (rustInternalCompilerError) {
            // In case of an internal error we return the entire stderr verbatim as a single error,
            // so we don't need to split it into errors
            return [rustInternalCompilerError]
          }
          const rustStderrPart: string[] = Array.from(stderr.matchAll(rustCompilerErrorRegex))
            .map((match) => match[1])
            .filter(
              (stderrPart) => !ignoredRustErrors.some((ignored) => stderrPart.startsWith(ignored))
            )
          const rustCompilerErrors = rustStderrPart.map(
            extractRustCompilerError(pipeline.name, source, getReport)
          )
          return rustCompilerErrors
        })(pipeline.compilerOutput.rust.stderr)
      )
    }
    if (pipeline.compilerOutput.systemError) {
      result.push.apply(
        result,
        ((systemErr) => [
          (() => ({
            name: `Error compiling ${pipeline.name}`,
            message: systemErr,
            cause: {
              entityName: pipeline.name,
              tag: 'programError',
              source,
              report: getReport(pipeline.name, systemErr),
              body: systemErr
            }
          }))()
        ])(pipeline.compilerOutput.systemError)
      )
    }
    return result
  }

const printSqlCompilerMessage = (message: SqlCompilerMessage) => {
  return `${message.warning ? 'warning' : 'error'}: ${message.error_type}
${message.message}
${message.snippet}
`
}

export const extractProgramStderr = (pipeline: { compilerOutput: CompilerOutput }) => {
  const result: string[] = []
  if (pipeline.compilerOutput.sql) {
    result.push(pipeline.compilerOutput.sql.messages.map(printSqlCompilerMessage).join('\n'))
    result.push(`SQL compiler exit code: ${pipeline.compilerOutput.sql.exit_code}`)
  }
  if (pipeline.compilerOutput.rust) {
    result.push(pipeline.compilerOutput.rust.stdout)
    result.push(pipeline.compilerOutput.rust.stderr)
    result.push(`Rust compiler exit code: ${pipeline.compilerOutput.rust.exit_code}`)
  }
  if (pipeline.compilerOutput.systemError) {
    result.push(pipeline.compilerOutput.systemError)
  }
  return result
}

export const programErrorsPerFile = <Report>(errors: SystemError<any, Report>[]) =>
  Object.fromEntries(
    groupBy(
      errors,
      (item) => item.cause.source.match(new RegExp(`#(${pipelineFileNameRegex})`))?.[1] ?? ''
    ).filter(([fileName]) => fileName !== '')
  )

export const pipelineFileNameRegex = '[\\w-_\\.]+'
