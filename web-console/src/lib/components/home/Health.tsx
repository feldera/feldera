// Should display aggregate health of all pipelines, just a placeholder right
// now.

import { nonNull } from '$lib/functions/common/function'
import { PipelineManagerQuery } from '$lib/services/defaultQueryFn'
import { Pipeline, ProgramDescr } from '$lib/services/manager'
import { match, P } from 'ts-pattern'

import { Icon } from '@iconify/react'
import { useClipboard } from '@mantine/hooks'
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  alpha,
  Box,
  IconButton,
  Link,
  Stack,
  useTheme
} from '@mui/material'
import Card from '@mui/material/Card'
import CardHeader from '@mui/material/CardHeader'
import Typography from '@mui/material/Typography'
import { useQuery } from '@tanstack/react-query'

const ProgramLink = ({ program }: { program: ProgramDescr }) => (
  <Link href={`/analytics/editor/?program_id=${program.program_id}`} target='_blank' rel='noreferrer'>
    @ {program.name}
  </Link>
)

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
                  Program SqlError <ProgramLink program={program} />
                  <br />
                  {program.program_id}
                </>
              )
            }
          })
      )
    )
    .with({ RustError: P.select() }, e => [
      new Error(e, {
        cause: {
          source: (
            <>
              Program RustError <ProgramLink program={program} />
              <br />
              {program.program_id}
            </>
          )
        }
      })
    ])
    .with({ SystemError: P.select() }, e => [
      new Error(e, {
        cause: {
          source: (
            <>
              Program SystemError <ProgramLink program={program} />
              <br />
              {program.program_id}
            </>
          )
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
                Pipeline Error{' '}
                <Link href={`/streaming/management/#${p.descriptor.pipeline_id}`} target='_blank' rel='noreferrer'>
                  @ {p.descriptor.name}
                </Link>
                <br />
                {p.descriptor.pipeline_id}
              </>
            )
          }
        })
      ]
    : []

const Health = () => {
  const theme = useTheme()
  const pipelinesQuery = useQuery(PipelineManagerQuery.pipeline())
  const programsQuery = useQuery(PipelineManagerQuery.program())

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
          <AccordionSummary expandIcon={<Icon icon='bx:chevron-down' fontSize={32} />}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 4, pr: 4, width: '100%' }}>
              <Icon icon='bx:error-circle' fontSize={20} />
              <Typography>Reported errors</Typography>
              <Typography variant='h6' sx={{ ml: 'auto' }}>
                {errors.length}
              </Typography>
            </Box>
          </AccordionSummary>
          {errors.length > 0 && (
            <AccordionDetails>
              <Stack spacing={4}>
                {errors.map((e, i) => {
                  const cause = (({ source, ...rest }) => {
                    void source
                    return JSON.stringify(rest, null, 2).replaceAll('\\n', '\n').replaceAll('\\"', '"')
                  })(e.cause ?? ({} as any))
                  return (
                    <Card key={i}>
                      <pre
                        style={{
                          padding: '0.5rem',
                          margin: '0',
                          fontSize: '14px',
                          backgroundColor: alpha('#888', 0.15)
                        }}
                      >
                        {(e.cause as any)?.['source']}
                      </pre>
                      <Box sx={{ p: 2, overflow: 'scroll', maxHeight: '10rem', width: '100%', height: '100%' }}>
                        <pre style={{ margin: '0', fontSize: '14px' }}>{e.message}</pre>
                      </Box>
                      {!['{}', ''].includes(cause) && (
                        <Box
                          sx={{
                            position: 'relative',
                            backgroundColor: alpha(theme.palette.error.main, 0.2)
                          }}
                        >
                          <Box sx={{ overflow: 'scroll', maxHeight: '10rem', width: '100%', height: '100%' }}>
                            <pre style={{ margin: '0', fontSize: '14px' }}>{cause}</pre>
                          </Box>
                          <IconButton
                            sx={{ position: 'absolute', top: 0, right: 0, mr: 4 }}
                            onClick={() => copy(cause)}
                            size='small'
                          >
                            <Icon icon='bx:copy' fontSize={24}></Icon>
                          </IconButton>
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
          <AccordionSummary expandIcon={<Icon icon='bx:chevron-down' fontSize={32} />}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 4, pr: 4, width: '100%' }}>
              <Icon icon='bx:error-circle' fontSize={20} />
              <Typography>Reported warnings</Typography>
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
