import { useSystemErrors } from '$lib/compositions/health/useSystemErrors'
import { SystemError } from '$lib/services/pipelineManagerAggregateQuery'
import PopupState, { bindHover, bindPopover } from 'material-ui-popup-state'
import HoverPopover from 'material-ui-popup-state/HoverPopover'
import { useState } from 'react'
import { sendGithubReport } from 'src/lib/services/feedback'
import JSONbig from 'true-json-bigint'

import { useClipboard } from '@mantine/hooks'
import {
  Badge,
  Box,
  Button,
  Collapse,
  IconButton,
  IconButtonProps,
  Link,
  List,
  ListItem,
  ListItemText,
  Tooltip,
  Typography
} from '@mui/material'

export function HealthPopup() {
  const { systemErrors } = useSystemErrors()
  return (
    <PopupState variant='popover' popupId='demo-popup-popover'>
      {popupState => (
        <div>
          {systemErrors.length ? (
            <Badge badgeContent={systemErrors.length} color='error'>
              <IconButton color='warning' {...bindHover(popupState)}>
                <i className='bx bx-error' style={{ fontSize: 24 }} />
              </IconButton>
            </Badge>
          ) : (
            <Tooltip title='No errors detected in Feldera deployment' slotProps={{ tooltip: { sx: { fontSize: 14 } } }}>
              <IconButton color='success'>
                <i className='bx bx-check-circle' style={{ fontSize: 24 }} />
              </IconButton>
            </Tooltip>
          )}
          <HoverPopover
            {...bindPopover(popupState)}
            anchorOrigin={{
              vertical: 'bottom',
              horizontal: 'center'
            }}
            transformOrigin={{
              vertical: 'top',
              horizontal: 'center'
            }}
            sx={{ mt: -5 }}
          >
            <HealthMenu systemErrors={systemErrors}></HealthMenu>
          </HoverPopover>
        </div>
      )}
    </PopupState>
  )
}

export function HealthMenu({ systemErrors }: { systemErrors: SystemError[] }) {
  const { copy } = useClipboard()
  const [expanded, setExpanded] = useState(null as number | null)
  return (
    <>
      <Typography variant='h6' sx={{ p: 2 }}>
        Feldera Health
      </Typography>
      <List sx={{ width: 640 }}>
        {systemErrors.map((error, index) => (
          <>
            <ListItem
              disablePadding
              onClick={() => setExpanded(expanded === index ? null : index)}
              secondaryAction={
                <Button
                  variant='outlined'
                  size='small'
                  sx={{ height: '100%', fontSize: 12 }}
                  onClick={e => {
                    e.stopPropagation()
                    sendGithubReport(error.cause.report)
                  }}
                >
                  report
                </Button>
              }
            >
              <IconButton data-testid={expanded === index ? 'button-collapse' : 'button-expand'}>
                {expanded === index ? <i className='bx bx-chevron-up' /> : <i className='bx bx-chevron-down' />}
              </IconButton>
              <ListItemText primary={<Link href={error.cause.source}>{error.name}</Link>} />
            </ListItem>
            <Typography variant='subtitle2' sx={{ px: 2 }}>
              {expanded === index ? '' : (str => (str.length <= 80 ? str : str.slice(0, 77) + '...'))(error.message)}
            </Typography>
            <Collapse
              in={expanded === index}
              collapsedSize={expanded === index ? '1rem' : undefined}
              timeout={200}
              unmountOnExit
              sx={{ px: 2 }}
            >
              <Typography variant='subtitle2'>{error.message}</Typography>
              <Box
                sx={{
                  position: 'relative'
                }}
              >
                <pre
                  style={{
                    padding: '0.5rem',
                    margin: '0',
                    fontSize: '14px',
                    backgroundColor: '#88888812',
                    position: 'relative',
                    overflowX: 'auto',
                    maxHeight: '12rem'
                  }}
                >
                  {JSONbig.stringify(error.cause.body, undefined, '\t').replaceAll('\\n', '\n').replaceAll('\\"', '"')}
                </pre>{' '}
                <CopyButton
                  onClick={() =>
                    copy(
                      JSONbig.stringify(error.cause.body, undefined, '\t')
                        .replaceAll('\\n', '\n')
                        .replaceAll('\\"', '"')
                    )
                  }
                ></CopyButton>
              </Box>
            </Collapse>
          </>
        ))}
      </List>
    </>
  )
}

const CopyButton = (props: IconButtonProps) => {
  return (
    <Box
      sx={{
        top: 0,
        right: 0,
        mx: 4,
        position: 'absolute',
        background: 'transparent'
      }}
    >
      <IconButton size='small' {...props}>
        <i className={`bx bx-copy`} style={{ fontSize: 20 }} />
      </IconButton>
    </Box>
  )
}
