import { describe, expect, it } from 'vitest'
import type { ExtendedPipeline, RustCompilerMessage } from '$lib/services/pipelineManager'
import { extractProgramErrors } from './systemErrors'

const getReport = (pipelineName: string, message: string) => ({ pipelineName, message })

const rustMessage = (
  partial: Partial<RustCompilerMessage> & Pick<RustCompilerMessage, 'message'>
): RustCompilerMessage => ({
  start_line_number: 1,
  start_column: 1,
  end_line_number: 1,
  end_column: 1,
  warning: false,
  error_type: 'error',
  file: null,
  rendered: null,
  ...partial
})

const withRust = (rust: {
  messages?: RustCompilerMessage[]
  stderr?: string
  stdout?: string
  exit_code?: number
}): Pick<ExtendedPipeline, 'name' | 'status' | 'compilerOutput'> => ({
  name: 'test-pipeline',
  status: 'Stopped',
  compilerOutput: {
    sql: undefined,
    rust: {
      exit_code: rust.exit_code ?? 101,
      stdout: rust.stdout ?? '',
      stderr: rust.stderr ?? '',
      messages: rust.messages
    },
    systemError: undefined
  }
})

const errorsOf = (rust: {
  messages?: RustCompilerMessage[]
  stderr?: string
  stdout?: string
  exit_code?: number
}) => extractProgramErrors(getReport)(withRust(rust))

const sccacheStderr = `error: process didn't exit successfully: \`sccache /home/ubuntu/.rustup/toolchains/1.93.1-aarch64-unknown-linux-gnu/bin/rustc -vV\` (exit status: 2)
--- stderr
sccache: error: Timed out waiting for server startup. Maybe the remote service is unreachable?
Run with SCCACHE_LOG=debug SCCACHE_NO_DAEMON=1 to get more information
`

describe('extractProgramErrors: structured rustc messages', () => {
  it('links a udf.rs span to the UDF editor tab', () => {
    const errors = errorsOf({
      messages: [
        rustMessage({
          file: '/tmp/compiler/udf.rs',
          start_line_number: 3,
          start_column: 5,
          end_line_number: 3,
          end_column: 11,
          error_type: 'E0433',
          message: 'failed to resolve: use of undeclared crate or module `chrnoo`',
          rendered:
            'error[E0433]: failed to resolve: use of undeclared crate or module `chrnoo`\n --> udf.rs:3:5\n'
        })
      ]
    })
    expect(errors).toHaveLength(1)
    expect(errors[0].cause.tag).toBe('programError')
    expect(errors[0].cause.source).toMatch(/#udf\.rs:3:5$/)
    expect(errors[0].cause.warning).toBe(false)
    expect(errors[0].cause.body).toMatchObject({
      startLineNumber: 3,
      startColumn: 5,
      endLineNumber: 3,
      endColumn: 11
    })
    expect(errors[0].message).toContain('error[E0433]')
  })

  it('links a stubs.rs warning to the stubs tab', () => {
    const errors = errorsOf({
      exit_code: 0,
      messages: [
        rustMessage({
          file: 'stubs.rs',
          start_line_number: 8,
          start_column: 9,
          warning: true,
          error_type: 'warning',
          message: 'unused variable: `x`'
        })
      ]
    })
    expect(errors).toHaveLength(1)
    expect(errors[0].cause.warning).toBe(true)
    expect(errors[0].cause.source).toMatch(/#stubs\.rs:8:9$/)
    expect(errors[0].message).toBe('warning: unused variable: `x`')
  })

  it('maps Cargo.toml line numbers onto udf.toml', () => {
    const errors = errorsOf({
      messages: [
        rustMessage({
          file: '/tmp/pipeline-globals/Cargo.toml',
          start_line_number: 14,
          start_column: 1,
          end_line_number: 14,
          end_column: 8,
          error_type: 'error',
          message: 'failed to parse manifest'
        })
      ]
    })
    expect(errors[0].cause.source).toMatch(/#udf\.toml:4:1$/)
    expect(errors[0].cause.body).toMatchObject({
      startLineNumber: 4,
      endLineNumber: 4
    })
  })

  it('keeps errors and warnings in the order they arrived', () => {
    const errors = errorsOf({
      messages: [
        rustMessage({
          file: 'udf.rs',
          error_type: 'E0425',
          message: 'cannot find value `x` in this scope',
          rendered: 'error[E0425]: cannot find value `x` in this scope'
        }),
        rustMessage({
          file: 'udf.rs',
          start_line_number: 2,
          warning: true,
          error_type: 'warning',
          message: 'unused import: `std::fmt`',
          rendered: 'warning: unused import: `std::fmt`'
        }),
        rustMessage({
          message: 'aborting due to 1 previous error',
          rendered: 'error: aborting due to 1 previous error'
        })
      ]
    })
    expect(errors.map((e) => e.message)).toEqual([
      'error[E0425]: cannot find value `x` in this scope',
      'warning: unused import: `std::fmt`',
      'error: aborting due to 1 previous error'
    ])
    expect(errors.map((e) => e.cause.warning)).toEqual([false, true, false])
    expect(errors[2].cause.tag).toBe('unrecognizedProgramError')
  })

  it('treats generated main.rs as an internal error on program.sql', () => {
    const errors = errorsOf({
      messages: [
        rustMessage({
          file: '/tmp/src/main.rs',
          start_line_number: 40,
          start_column: 1,
          message: 'internal compiler failure'
        })
      ]
    })
    expect(errors).toHaveLength(1)
    expect(errors[0].cause.tag).toBe('programError')
    expect(errors[0].cause.source).toMatch(/#program\.sql$/)
    expect(errors[0].cause.body).toMatchObject({
      startLineNumber: 0,
      endLineNumber: 9999
    })
  })

  it('does not invent errors from cargo progress on a successful compile', () => {
    const errors = errorsOf({
      exit_code: 0,
      messages: [],
      stderr: '   Compiling feldera-sqllib v0.1.0\n    Finished release profile\n'
    })
    expect(errors).toEqual([])
  })

  it('ignores cargo chatter on stderr when messages are present', () => {
    const errors = errorsOf({
      stderr: 'error: could not compile `feldera` (lib) due to 1 previous error\n',
      messages: [
        rustMessage({
          file: 'udf.rs',
          message: 'cannot find value `x` in this scope',
          rendered: 'error[E0425]: cannot find value `x` in this scope'
        })
      ]
    })
    expect(errors.map((e) => e.message)).toEqual([
      'error[E0425]: cannot find value `x` in this scope'
    ])
  })
})

const withRustStderr = (
  stderr: string
): Pick<ExtendedPipeline, 'name' | 'status' | 'compilerOutput'> => ({
  name: 'test-pipeline',
  status: 'Stopped',
  compilerOutput: {
    sql: undefined,
    rust: { exit_code: 101, stdout: '', stderr },
    systemError: undefined
  }
})

const stderrErrorsOf = (stderr: string) => extractProgramErrors(getReport)(withRustStderr(stderr))

describe('extractProgramErrors: stderr when messages are absent', () => {
  it('links a udf.rs span from an old manager', () => {
    const errors = stderrErrorsOf(`error[E0433]: cannot find module or crate \`chrnoo\`
 --> /tmp/compiler/udf.rs:2:5
  |
2 |     chrnoo::Utc::now();
  |     ^^^^^^
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].cause.source).toMatch(/#udf\.rs:2:5$/)
    expect(errors[0].cause.tag).toBe('programError')
  })

  it('reports a message that is the last paragraph of stderr', () => {
    const errors = stderrErrorsOf(`error: linking with \`cc\` failed: exit status: 1
  = note: collect2: error: ld returned 1 exit status
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toContain('error: linking with `cc` failed')
  })

  it('reports a message that ends stderr without a trailing newline', () => {
    const errors = stderrErrorsOf('error: linking with `cc` failed: exit status: 1')
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toBe('error: linking with `cc` failed: exit status: 1')
  })

  it('keeps the last message when earlier ones are blank-line separated', () => {
    const errors = stderrErrorsOf(`error[E0425]: cannot find value \`x\` in this scope
 --> src/lib.rs:1:1

warning: unused import: \`std::fmt\`
 --> src/lib.rs:2:5

error: aborting due to 1 previous error
`)
    expect(errors.map((e) => e.message.split('\n')[0])).toEqual([
      'error[E0425]: cannot find value `x` in this scope',
      'warning: unused import: `std::fmt`',
      'error: aborting due to 1 previous error'
    ])
    expect(errors.map((e) => e.cause.warning)).toEqual([false, true, false])
  })

  it('does not split a message on its own interior line breaks', () => {
    const errors = stderrErrorsOf(`error: linking with \`cc\` failed: exit status: 1
  = note: some arguments are omitted
  = note: collect2: error: ld returned 1 exit status
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].message.split('\n')).toHaveLength(3)
  })

  it('ignores black-listed warnings', () => {
    const errors = stderrErrorsOf(
      `warning: patch for the non root package will be ignored, specify patch at the workspace root
`
    )
    expect(errors).toEqual([])
  })

  it('returns an empty list for stderr without messages', () => {
    const errors = stderrErrorsOf(
      '   Compiling feldera-sqllib v0.1.0\n    Finished release profile\n'
    )
    expect(errors).toEqual([])
  })

  it('reports the sccache failure without the cargo message that wraps it', () => {
    const errors = stderrErrorsOf(sccacheStderr)
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toBe(
      `sccache: error: Timed out waiting for server startup. Maybe the remote service is unreachable?
Run with SCCACHE_LOG=debug SCCACHE_NO_DAEMON=1 to get more information`
    )
    expect(errors[0].cause.tag).toBe('unrecognizedProgramError')
  })

  it('reports echoed stderr that ends stderr without a trailing newline', () => {
    const errors = stderrErrorsOf(sccacheStderr.trimEnd())
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toMatch(/^sccache: error: Timed out waiting for server startup/)
  })

  it('skips echoed stdout preceding the echoed stderr', () => {
    const errors =
      stderrErrorsOf(`error: process didn't exit successfully: \`sccache rustc -vV\` (exit status: 2)
--- stdout
rustc 1.93.1
--- stderr
sccache: error: Timed out waiting for server startup.
`)
    expect(errors.map((e) => e.message)).toEqual([
      'sccache: error: Timed out waiting for server startup.'
    ])
  })

  it('classifies a tool-prefixed warning as a warning', () => {
    const errors = stderrErrorsOf(`error: could not compile \`feldera\` (lib)
--- stderr
sccache: warning: reached the local cache size limit
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].cause.warning).toBe(true)
    expect(errors[0].message).toBe('sccache: warning: reached the local cache size limit')
  })

  it('drops an ignored cargo message that echoes nothing', () => {
    const errors =
      stderrErrorsOf(`error: could not compile \`feldera\` (lib) due to 1 previous error
`)
    expect(errors).toEqual([])
  })

  it('does not start a new message on an indented tool-prefixed line', () => {
    const errors =
      stderrErrorsOf(`error[E0433]: failed to resolve: use of undeclared crate \`serde\`
 --> src/lib.rs:1:5
  = note: sccache: error: this note is part of the message, not a new message
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toMatch(/^error\[E0433\]: failed to resolve/)
    expect(errors[0].message).toContain('not a new message')
  })

  it('treats an empty messages list like a missing one', () => {
    const errors = errorsOf({ messages: [], stderr: sccacheStderr })
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toMatch(/^sccache: error:/)
  })
})
