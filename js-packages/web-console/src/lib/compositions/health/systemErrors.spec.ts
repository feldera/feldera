/**
 * Unit tests for `extractProgramErrors` on Rust compiler stderr.
 *
 * The console only renders errors this function extracts, so a message the
 * splitting regex misses is invisible unless the user turns on verbatim errors.
 * The cases below cover the message terminators cargo emits (a blank line, the
 * next message, the end of stderr) and the subprocess stderr cargo echoes below
 * a `--- stderr` marker.
 */
import { describe, expect, it } from 'vitest'
import type { ExtendedPipeline } from '$lib/services/pipelineManager'
import { extractProgramErrors } from './systemErrors'

const getReport = (pipelineName: string, message: string) => ({ pipelineName, message })

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

const errorsOf = (stderr: string) => extractProgramErrors(getReport)(withRustStderr(stderr))

// Verbatim sccache failure from a report: the cargo message that wraps it is
// the last one in stderr, ending in a single newline rather than a blank line.
const sccacheStderr = `error: process didn't exit successfully: \`sccache /home/ubuntu/.rustup/toolchains/1.93.1-aarch64-unknown-linux-gnu/bin/rustc -vV\` (exit status: 2)
--- stderr
sccache: error: Timed out waiting for server startup. Maybe the remote service is unreachable?
Run with SCCACHE_LOG=debug SCCACHE_NO_DAEMON=1 to get more information
`

describe('extractProgramErrors: splitting Rust stderr into messages', () => {
  it('reports a message that is the last paragraph of stderr', () => {
    const errors = errorsOf(`error: linking with \`cc\` failed: exit status: 1
  = note: collect2: error: ld returned 1 exit status
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toContain('error: linking with `cc` failed')
  })

  it('reports a message that ends stderr without a trailing newline', () => {
    const errors = errorsOf('error: linking with `cc` failed: exit status: 1')
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toBe('error: linking with `cc` failed: exit status: 1')
  })

  it('keeps the last message when earlier ones are blank-line separated', () => {
    const errors = errorsOf(`error[E0425]: cannot find value \`x\` in this scope
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
    const errors = errorsOf(`error: linking with \`cc\` failed: exit status: 1
  = note: some arguments are omitted
  = note: collect2: error: ld returned 1 exit status
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].message.split('\n')).toHaveLength(3)
  })

  it('ignores black-listed warnings', () => {
    const errors =
      errorsOf(`warning: patch for the non root package will be ignored, specify patch at the workspace root
`)
    expect(errors).toEqual([])
  })

  it('returns an empty list for stderr without messages', () => {
    const errors = errorsOf('   Compiling feldera-sqllib v0.1.0\n    Finished release profile\n')
    expect(errors).toEqual([])
  })
})

describe('extractProgramErrors: subprocess stderr echoed by cargo', () => {
  it('reports the sccache failure without the cargo message that wraps it', () => {
    const errors = errorsOf(sccacheStderr)
    expect(errors).toHaveLength(1)
    expect(
      errors[0].message
    ).toBe(`sccache: error: Timed out waiting for server startup. Maybe the remote service is unreachable?
Run with SCCACHE_LOG=debug SCCACHE_NO_DAEMON=1 to get more information`)
    expect(errors[0].cause.warning).toBe(false)
    expect(errors[0].cause.tag).toBe('unrecognizedProgramError')
  })

  it('reports echoed stderr that ends stderr without a trailing newline', () => {
    const errors = errorsOf(sccacheStderr.trimEnd())
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toMatch(/^sccache: error: Timed out waiting for server startup/)
  })

  it('skips echoed stdout preceding the echoed stderr', () => {
    const errors =
      errorsOf(`error: process didn't exit successfully: \`sccache rustc -vV\` (exit status: 2)
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
    const errors = errorsOf(`error: could not compile \`feldera\` (lib)
--- stderr
sccache: warning: reached the local cache size limit
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].cause.warning).toBe(true)
    expect(errors[0].message).toBe('sccache: warning: reached the local cache size limit')
  })

  it('drops an ignored cargo message that echoes nothing', () => {
    const errors = errorsOf(`error: could not compile \`feldera\` (lib) due to 1 previous error
`)
    expect(errors).toEqual([])
  })

  it('does not start a new message on an indented tool-prefixed line', () => {
    const errors = errorsOf(`error[E0433]: failed to resolve: use of undeclared crate \`serde\`
 --> src/lib.rs:1:5
  = note: sccache: error: this note is part of the message, not a new message
`)
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toMatch(/^error\[E0433\]: failed to resolve/)
    expect(errors[0].message).toContain('not a new message')
  })
})
