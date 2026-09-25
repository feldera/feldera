import { count, groupBy } from '$lib/functions/common/array'
import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
import { resolve } from '$lib/functions/svelte'
import { defaultGithubReportSections, type ReportDetails } from '$lib/services/githubReport'
import type {
  CompilerOutput,
  ExtendedPipeline,
  Pipeline,
  RustCompilerMessage,
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

export const showSqlCompilerMessage = (e: SqlCompilerMessage) =>
  `${e.warning ? 'warning' : 'error'}: ${e.error_type ? e.error_type + '\n' : ''}${e.message}${e.snippet ? '\n' + e.snippet : ''}`

export const showRustCompilerMessage = (e: RustCompilerMessage) => {
  const rendered = e.rendered?.trimEnd()
  if (rendered) {
    return rendered
  }
  const level = e.warning ? 'warning' : 'error'
  if (!e.error_type || e.error_type === level) {
    return `${level}: ${e.message}`
  }
  return `${level}: ${e.error_type}\n${e.message}`
}

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

/** Generated `Cargo.toml` line numbers are 10 ahead of the user's `udf.toml`. */
const CARGO_TOML_TO_UDF_TOML_LINE_OFFSET = -10

type RustEditorTarget = {
  fileName: string
  startLineNumber: number
  startColumn: number
  endLineNumber: number
  endColumn: number
}

const rustEditorTarget = (e: RustCompilerMessage): RustEditorTarget | 'internal' | null => {
  const base = e.file?.replaceAll('\\', '/').split('/').pop()
  if (!base) {
    return null
  }
  if (base === 'udf.rs' || base === 'stubs.rs') {
    return {
      fileName: base,
      startLineNumber: e.start_line_number,
      startColumn: e.start_column,
      endLineNumber: e.end_line_number,
      endColumn: e.end_column
    }
  }
  if (base === 'Cargo.toml') {
    return {
      fileName: 'udf.toml',
      startLineNumber: e.start_line_number + CARGO_TOML_TO_UDF_TOML_LINE_OFFSET,
      startColumn: e.start_column,
      endLineNumber: e.end_line_number + CARGO_TOML_TO_UDF_TOML_LINE_OFFSET,
      endColumn: e.end_column
    }
  }
  return 'internal'
}

const extractRustCompilerMessage =
  <Report>(
    pipelineName: string,
    source: string,
    getReport: (pipelineName: string, message: string) => Report
  ) =>
  (e: RustCompilerMessage): SystemError<any, Report> => {
    const text = showRustCompilerMessage(e)
    const target = rustEditorTarget(e)
    const warning = e.warning
    const name = `${warning ? 'Warning in' : 'Error compiling'} ${pipelineName}`
    const rangeBody = (loc: {
      startLineNumber: number
      startColumn: number
      endLineNumber: number
      endColumn: number
    }) => ({
      startLineNumber: loc.startLineNumber,
      endLineNumber: loc.endLineNumber,
      startColumn: loc.startColumn,
      endColumn: loc.endColumn,
      message: text
    })

    if (target && target !== 'internal') {
      return {
        name,
        message: text,
        cause: {
          entityName: pipelineName,
          tag: 'programError',
          source:
            source +
            `#${target.fileName}:` +
            target.startLineNumber +
            (target.startColumn > 0 ? ':' + target.startColumn.toString() : ''),
          report: getReport(pipelineName, text),
          body: rangeBody(target),
          warning
        }
      }
    }

    if (target === 'internal') {
      return {
        name: `Error compiling ${pipelineName}`,
        message: text,
        cause: {
          entityName: pipelineName,
          tag: 'programError',
          source: source + '#program.sql',
          report: getReport(pipelineName, text),
          body: rangeBody({
            startLineNumber: 0,
            startColumn: 0,
            endLineNumber: 9999,
            endColumn: 9999
          }),
          warning
        }
      }
    }

    return {
      name,
      message: text,
      cause: {
        entityName: pipelineName,
        tag: 'unrecognizedProgramError',
        source: source + '#program.sql',
        report: getReport(pipelineName, text),
        body: warning
          ? text
          : rangeBody({
              startLineNumber: 0,
              startColumn: 0,
              endLineNumber: 9999,
              endColumn: 9999
            }),
        warning
      }
    }
  }

const extractRustCompilerError =
  <Report>(
    pipelineName: string,
    source: string,
    getReport: (pipelineName: string, message: string) => Report
  ) =>
  (stderr: string): SystemError<any, Report> => {
    const matchFileError = (
      fileName: string,
      fileRegex: RegExp,
      lineOffset: number,
      warning: boolean
    ) => {
      const match = stderr.match(fileRegex)
      if (!match) {
        return undefined
      }
      const startLineNumber = parseInt(match[1]) + lineOffset
      const startColumn = parseInt(match[2])
      return {
        name: `${warning ? 'Warning in' : 'Error compiling'} ${pipelineName}`,
        message: stderr,
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
    const warning = /^(?:[\w-]+: )?warning:/.test(stderr)
    return (
      matchFileError('udf.toml', /\/Cargo\.toml:(\d+):(\d+)/, -10, warning) ??
      matchFileError('udf.rs', /\/udf\.rs:(\d+):(\d+)/, 0, warning) ??
      matchFileError('stubs.rs', /\/stubs\.rs:(\d+):(\d+)/, 0, warning) ?? {
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
    )
  }

const ignoredRustErrors = [
  'warning: patch for the non root package will be ignored',
  'error: could not compile',
  'warning: build failed, waiting for other jobs to finish',
  "error: process didn't exit successfully"
]

/** Split cargo's `--- stderr` marker so the echoed tool error is its own message. */
const flattenStderr = (stderr: string) => stderr.replaceAll('\n--- stderr\n', '\n\n')

const rustCompilerErrorRegex =
  /^((?:[\w-]+: )?(warning:(?! `)|error(\[[\w]+\])?:)([\s\S])+?)(\n(\n|(?=error|warning))|\n?$(?![\s\S]))/gm

/** Old managers put rustc text in stderr and do not send `messages`. */
const extractRustStderrErrors = <Report>(
  stderr: string,
  pipelineName: string,
  source: string,
  getReport: (pipelineName: string, message: string) => Report
): SystemError<any, Report>[] => {
  const internal = extractInternalCompilationError(stderr, pipelineName, source, getReport)
  if (internal) {
    return [internal]
  }
  return Array.from(flattenStderr(stderr).matchAll(rustCompilerErrorRegex))
    .map((match) => match[1])
    .filter((stderrPart) => !ignoredRustErrors.some((ignored) => stderrPart.startsWith(ignored)))
    .map(extractRustCompilerError(pipelineName, source, getReport))
}

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
      const rust = pipeline.compilerOutput.rust
      const messages = rust.messages ?? []
      if (messages.length > 0) {
        result.push.apply(
          result,
          messages.map(extractRustCompilerMessage(pipeline.name, source, getReport))
        )
      } else {
        result.push.apply(
          result,
          extractRustStderrErrors(rust.stderr, pipeline.name, source, getReport)
        )
      }
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

const printRustCompilerMessage = (message: RustCompilerMessage) => {
  return showRustCompilerMessage(message) + '\n'
}

export const extractProgramStderr = (pipeline: { compilerOutput: CompilerOutput }) => {
  const result: string[] = []
  if (pipeline.compilerOutput.sql) {
    result.push(pipeline.compilerOutput.sql.messages.map(printSqlCompilerMessage).join('\n'))
    result.push(`SQL compiler exit code: ${pipeline.compilerOutput.sql.exit_code}`)
  }
  if (pipeline.compilerOutput.rust) {
    result.push(
      (pipeline.compilerOutput.rust.messages ?? []).map(printRustCompilerMessage).join('\n')
    )
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
