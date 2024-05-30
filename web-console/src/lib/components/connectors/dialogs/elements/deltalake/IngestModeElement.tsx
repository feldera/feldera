import { BigNumberElement, BigNumberInputProps } from '$lib/components/input/BigNumberInput'
import { SQLValueElement } from '$lib/components/streaming/import/SQLValueInput'
import BigNumber from 'bignumber.js'
import { useWatch } from 'react-hook-form'
import { SelectElement, TextareaAutosizeElement } from 'react-hook-form-mui'
import Markdown from 'react-markdown'
import { GridItems } from 'src/lib/components/common/GridItems'
import { match } from 'ts-pattern'

import { BaseTextFieldProps, Box, Grid, Tooltip, useTheme } from '@mui/material'

export const DeltaLakeIngestModeElement = (props: { parentName: string }) => {
  const theme = useTheme()
  const tooltipProps = {
    slotProps: {
      tooltip: {
        sx: {
          backgroundColor: theme.palette.background.default,
          color: theme.palette.text.primary,
          fontSize: 14,
          width: '500'
        }
      }
    }
  }
  const mode: 'snapshot' | 'follow' | 'snapshot_and_follow' = useWatch({ name: props.parentName + '.mode' })
  return (
    <>
      <Grid container spacing={4} sx={{ alignItems: 'center' }}>
        <GridItems xs={12}>
          <SelectElement
            name={props.parentName + '.mode'}
            label='ingest mode'
            size='small'
            fullWidth
            options={[
              {
                id: 'snapshot',
                label: 'snapshot'
              },
              {
                id: 'follow',
                label: 'follow'
              },
              {
                id: 'snapshot_and_follow',
                label: 'snapshot_and_follow'
              }
            ]}
            inputProps={{
              'data-testid': 'input-ingest-mode'
            }}
            helperText={match(mode)
              .with('follow', () => 'Follow the changelog of the table, only ingesting changes (new and deleted rows).')
              .with('snapshot', () => 'Read a snapshot of the table and stop.')
              .with('snapshot_and_follow', () => 'Take a snapshot of the table before switching to the `follow` mode.')
              .exhaustive()}
          ></SelectElement>
          {match(mode)
            .with('snapshot', () => (
              <>
                <FilterElement parentName={props.parentName} tooltipProps={tooltipProps} />
                <VersionElement
                  parentName={props.parentName}
                  label='snapshot version'
                  inputProps={{
                    'data-testid': 'input-snapshot-version'
                  }}
                  tooltipProps={tooltipProps}
                ></VersionElement>
                <DatetimeElement
                  parentName={props.parentName}
                  label='snapshot datetime'
                  inputProps={{
                    'data-testid': 'input-snapshot-datetime'
                  }}
                  tooltipProps={tooltipProps}
                />
              </>
            ))
            .with('follow', () => (
              <>
                <VersionElement
                  parentName={props.parentName}
                  label='start version'
                  inputProps={{
                    'data-testid': 'input-start-version'
                  }}
                  tooltipProps={tooltipProps}
                ></VersionElement>
                <DatetimeElement
                  parentName={props.parentName}
                  label='start datetime'
                  inputProps={{
                    'data-testid': 'input-start-datetime'
                  }}
                  tooltipProps={tooltipProps}
                />
              </>
            ))
            .with('snapshot_and_follow', () => (
              <>
                <FilterElement parentName={props.parentName} tooltipProps={tooltipProps} />
                <VersionElement
                  parentName={props.parentName}
                  label='start version'
                  inputProps={{
                    'data-testid': 'input-start-version'
                  }}
                  tooltipProps={tooltipProps}
                ></VersionElement>
                <DatetimeElement
                  parentName={props.parentName}
                  label='start datetime'
                  inputProps={{
                    'data-testid': 'input-start-datetime'
                  }}
                  tooltipProps={tooltipProps}
                />
              </>
            ))
            .exhaustive()}
        </GridItems>
      </Grid>
    </>
  )
}

const maxI64 = BigNumber('9223372036854775807')

const VersionElement = ({
  parentName,
  tooltipProps,
  ...props
}: { parentName: string; tooltipProps: any } & Omit<BaseTextFieldProps, 'component'> & BigNumberInputProps) => (
  <Tooltip
    {...tooltipProps}
    title={
      <Markdown>
        {
          '\
Optional table version.\n\n\
When this option is set, the connector finds and opens the specified version of the table.\
In `snapshot` and `snapshot_and_follow` modes, it retrieves the snapshot of this version of\
the table.  In `follow` and `snapshot_and_follow` modes, it follows transaction log records\
**after** this version.\n\n\
Note: at most one of `version` and `datetime` options can be specified.\
When neither of the two options is specified, the latest committed version of the table\
is used.'
        }
      </Markdown>
    }
  >
    <Box>
      <BigNumberElement
        name={parentName + '.version'}
        size='small'
        fullWidth
        min={0}
        max={maxI64}
        {...props}
        optional
      ></BigNumberElement>
    </Box>
  </Tooltip>
)
const DatetimeElement = (props: { parentName: string; tooltipProps: any } & BaseTextFieldProps) => (
  <Tooltip
    {...props.tooltipProps}
    title={
      <Markdown>
        {
          '\
Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format.\n\n\
When this option is set, the connector finds and opens the version of the table as of the\
specified point in time.  In `snapshot` and `snapshot_and_follow` modes, it retrieves the\
snapshot of this version of the table.  In `follow` and `snapshot_and_follow` modes, it\
follows transaction log records **after** this version.\n\n\
Note: at most one of `version` and `datetime` options can be specified.\
When neither of the two options is specified, the latest committed version of the table\
is used.'
        }
      </Markdown>
    }
  >
    <Box>
      <SQLValueElement
        name={props.parentName + '.datetime'}
        size='small'
        fullWidth
        focused
        columnType={{ type: 'TIMESTAMP', nullable: false }}
        {...props}
      ></SQLValueElement>
    </Box>
  </Tooltip>
)

const FilterElement = (props: { parentName: string; tooltipProps: any }) => (
  <Tooltip
    {...props.tooltipProps}
    title={
      <Markdown>
        {
          "\
Optional row filter.\n\n\
This option is only valid when `mode` is set to `snapshot` or `snapshot_and_follow`.\n\n\
When specified, only rows that satisfy the filter condition are included in the\
snapshot.  The condition must be a valid SQL Boolean expression that can be used in\
the `where` clause of the `select * from snapshot where ...` query.\n\n\
This option can be used to specify the range of event times to include in the snapshot,\
e.g.: `ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'`."
        }
      </Markdown>
    }
  >
    <TextareaAutosizeElement
      resizeStyle='vertical'
      name={props.parentName + '.filter'}
      label='snapshot filter'
      size='small'
      fullWidth
      inputProps={{
        'data-testid': 'input-snapshot-filter'
      }}
      helperText='Boolean SQL expression'
      {...props}
    ></TextareaAutosizeElement>
  </Tooltip>
)
