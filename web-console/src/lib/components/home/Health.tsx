// Should display aggregate health of all pipelines, just a placeholder right
// now.

import { ReportErrorButton } from '$lib/components/home/health/ReportErrorButton'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { nonNull } from '$lib/functions/common/function'
import { ProgramDescr, ProgramsService } from '$lib/services/manager'
import { Pipeline } from '$lib/types/pipeline'
import { match, P } from 'ts-pattern'

import { useClipboard } from '@mantine/hooks'
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  alpha,
  Box,
  IconButton,
  IconButtonProps,
  Link,
  Stack,
  useTheme
} from '@mui/material'
import Card from '@mui/material/Card'
import CardHeader from '@mui/material/CardHeader'
import Typography from '@mui/material/Typography'
import { useQuery } from '@tanstack/react-query'

const ProgramLink = ({ program }: { program: ProgramDescr }) => (
  <Link href={`/analytics/editor/?program_name=${program.name}`}>{program.name || 'Unnamed program'}</Link>
)

const limitMessage = (text: string | null | undefined, max: number, prefix: string) =>
  (t => (t.length > max ? prefix : '') + t.slice(Math.max(0, t.length - max)))(text || '')

const programErrors = (program: ProgramDescr) =>
  match(program.status)
    .returnType<Error[]>()
    .with({ SqlError: P.select() }, es =>
      es.map(
        e =>
          new Error(e.message, {
            cause: {
              ...e,
              source: (
                <>
                  SQL Error
                  <br />
                  <ProgramLink program={program} />
                  <br />
                  {program.program_id}
                </>
              ),
              report: {
                Error: '```\n' + limitMessage(e.message, 1000, '\n...Beginning of the error...') + '\n```',
                SQL: () =>
                  ProgramsService.getProgram(program.name, true).then(
                    p => '```\n' + limitMessage(p.code, 7000, '\n...Beginning of the code...') + '\n```'
                  )
              }
            }
          })
      )
    )
    .with({ RustError: P.select() }, (e: string) => [
      new Error(e, {
        cause: {
          source: (
            <>
              System Error
              <br />
              <ProgramLink program={program} />
              <br />
              {program.program_id}
            </>
          ),
          report: {
            Error: '```\n' + limitMessage(e, 1000, '\n...Beginning of the error...') + '\n```',
            SQL: () =>
              ProgramsService.getProgram(program.name, true).then(
                p => '```\n' + limitMessage(p.code, 7000, '\n...Beginning of the code...') + '\n```'
              )
          }
        }
      })
    ])
    .with({ SystemError: P.select() }, (e: string) => [
      new Error(e, {
        cause: {
          source: (
            <>
              System Error
              <br />
              <ProgramLink program={program} />
              <br />
              {program.program_id}
            </>
          ),
          report: {
            Error: '```\n' + limitMessage(e, 1000, '\n...Beginning of the error...') + '\n```',
            SQL: () =>
              ProgramsService.getProgram(program.name, true).then(
                p => '```\n' + limitMessage(p.code, 7000, '\n...Beginning of the code...') + '\n```'
              )
          }
        }
      })
    ])
    .with(P._, () => [])
    .exhaustive()

const pipelineErrors = (p: Pipeline) =>
  nonNull(p.state.error)
    ? [
        new Error(p.state.error.message, {
          cause: {
            ...p.state.error,
            source: (
              <>
                Pipeline Error
                <br />
                <Link href={`/streaming/management/#${p.descriptor.name}`}>
                  {p.descriptor.name || 'Unnamed pipeline'}
                </Link>
                <br />
                {p.descriptor.pipeline_id}
              </>
            ),
            report: {
              Error: '```\n' + limitMessage(p.state.error.message, 1000, '\n...Beginning of the error...') + '\n```',
              Version: String(p.descriptor.version),
              ...(programName =>
                !programName
                  ? {}
                  : {
                      SQL: () =>
                        ProgramsService.getProgram(programName, true).then(
                          program =>
                            '```\n' + limitMessage(program.code, 7000, '\n...Beginning of the code...') + '\n```'
                        )
                    })(p.descriptor.program_name)
            }
          }
        })
      ]
    : []

const CopyButton = (props: IconButtonProps) => {
  const theme = useTheme()
  return (
    <Box
      sx={{
        top: 0,
        right: 0,
        mx: 5,
        my: 2,
        position: 'absolute',
        background: theme.palette.background.paper,
        border: 'solid',
        borderRadius: '0.25rem'
      }}
    >
      <IconButton size='small' {...props}>
        <i className={`bx bx-copy`} style={{ fontSize: 16 }} />
      </IconButton>
    </Box>
  )
}

const Health = () => {
  const theme = useTheme()
  const pipelineManagerQuery = usePipelineManagerQuery()
  const pipelinesQuery = useQuery(pipelineManagerQuery.pipelines())
  const programsQuery = useQuery(pipelineManagerQuery.programs())

  const errors = [
    ...(pipelinesQuery.isError ? [pipelinesQuery.error as Error] : (pipelinesQuery.data ?? []).flatMap(pipelineErrors)),
    ...(programsQuery.isError ? [programsQuery.error as Error] : (programsQuery.data ?? []).flatMap(programErrors))
  ]
  const { copy } = useClipboard()
  return (
    <Box sx={{ position: 'relative', height: '12rem' }}>
      <Card
        sx={{
          position: 'absolute',
          width: '100%',
          mb: '10rem',
          maxHeight: 'calc(100vh - 9rem)',
          overflow: 'auto',
          scrollbarWidth: 'none'
        }}
      >
        <CardHeader title='Feldera Health'></CardHeader>
        <Accordion disableGutters>
          <AccordionSummary expandIcon={<i className={`bx bx-chevron-down`} style={{ fontSize: 24 }} />}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 4, pr: 4, width: '100%' }}>
              <i className={`bx bx-error-circle`} style={{ fontSize: 20 }} />
              <Typography>Platform errors</Typography>
              <Typography variant='h6' sx={{ ml: 'auto' }}>
                {errors.length}
              </Typography>
            </Box>
          </AccordionSummary>
          {errors.length > 0 && (
            <AccordionDetails>
              <Stack spacing={4}>
                {errors.map((e, i) => {
                  const cause = e.cause ?? ({} as any)
                  const causeStr = (({ source, report, ...rest }) => {
                    void source, report
                    return JSON.stringify(rest, null, 2).replaceAll('\\n', '\n').replaceAll('\\"', '"')
                  })(cause)
                  return (
                    <Card key={i}>
                      <pre
                        style={{
                          padding: '0.5rem',
                          margin: '0',
                          fontSize: '14px',
                          backgroundColor: alpha('#888', 0.15),
                          position: 'relative'
                        }}
                      >
                        {cause?.source}
                        {cause?.report && <ReportErrorButton report={cause.report} />}
                      </pre>
                      <Box
                        sx={{
                          position: 'relative'
                        }}
                      >
                        <Box
                          sx={{
                            p: 2,
                            overflow: 'scroll',
                            paddingTop: '3rem',
                            minHeight: '4rem',
                            maxHeight: '10rem',
                            width: '100%',
                            height: '100%'
                          }}
                        >
                          <pre style={{ margin: '0', fontSize: '14px' }}>{e.message}</pre>
                        </Box>

                        <CopyButton onClick={() => copy(e.message)}></CopyButton>
                      </Box>
                      {!['{}', ''].includes(causeStr) && (
                        <Box
                          sx={{
                            position: 'relative',
                            backgroundColor: alpha(theme.palette.error.main, 0.2)
                          }}
                        >
                          <Box
                            sx={{
                              overflow: 'scroll',
                              minHeight: '4rem',
                              maxHeight: '10rem',
                              width: '100%',
                              height: '100%'
                            }}
                          >
                            <pre style={{ margin: '0', fontSize: '14px' }}>{causeStr}</pre>
                          </Box>
                          <CopyButton onClick={() => copy(causeStr)}></CopyButton>
                        </Box>
                      )}
                    </Card>
                  )
                })}
              </Stack>
            </AccordionDetails>
          )}
        </Accordion>
        <Accordion disableGutters>
          <AccordionSummary expandIcon={<i className={`bx bx-chevron-down`} style={{ fontSize: 24 }} />}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 4, pr: 4, width: '100%' }}>
              <i className={`bx bx-error-circle`} style={{ fontSize: 20 }} />
              <Typography>Platform warnings</Typography>
              <Typography variant='h6' sx={{ ml: 'auto' }}>
                0
              </Typography>
            </Box>
          </AccordionSummary>
        </Accordion>
      </Card>
    </Box>
  )
}

export default Health
