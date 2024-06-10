import { FormTooltip } from '$lib/components/forms/Tooltip'
import { BigNumberElement, BigNumberInputProps } from '$lib/components/input/BigNumberInput'
import { SQLValueElement } from '$lib/components/streaming/import/SQLValueInput'
import BigNumber from 'bignumber.js/bignumber.js'
import { ChangeEvent } from 'react'
import { useFormContext, useWatch } from 'react-hook-form'
import { SelectElement, TextareaAutosizeElement, TextFieldElement } from 'react-hook-form-mui'
import { GridItems } from 'src/lib/components/common/GridItems'
import { match } from 'ts-pattern'

import { BaseTextFieldProps, Box, Grid, TextFieldProps } from '@mui/material'

export const DeltaLakeIngestModeElement = (props: { parentName: string }) => {
  const mode: 'snapshot' | 'follow' | 'snapshot_and_follow' = useWatch({ name: props.parentName + '.mode' })
  const ctx = useFormContext()
  const unregisterConflicting = (field: string) => (event: ChangeEvent<HTMLInputElement>) => {
    if (!event.target.value) return
    ctx.setValue(field, undefined)
  }
  return (
    <>
      <Grid container spacing={4} sx={{ alignItems: 'center' }}>
        <GridItems xs={12}>
          <SelectElement
            name={props.parentName + '.mode'}
            label='Ingest mode'
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
            InputProps={
              {
                'data-testid': 'input-ingest-mode'
              } as any
            }
            helperText={match(mode)
              .with('follow', () => 'Follow the changelog of the table, only ingesting changes (new and deleted rows).')
              .with('snapshot', () => 'Read a snapshot of the table and stop.')
              .with('snapshot_and_follow', () => 'Take a snapshot of the table before switching to the `follow` mode.')
              .exhaustive()}
          ></SelectElement>
          {match(mode)
            .with('snapshot', () => (
              <>
                <FilterElement parentName={props.parentName} />
                <VersionElement
                  parentName={props.parentName}
                  label='Snapshot version'
                  inputProps={{
                    'data-testid': 'input-version'
                  }}
                  onInput={unregisterConflicting(props.parentName + '.datetime')}
                ></VersionElement>
                <DatetimeElement
                  parentName={props.parentName}
                  label='Snapshot datetime'
                  inputProps={{
                    'data-testid': 'input-datetime'
                  }}
                  onInput={unregisterConflicting(props.parentName + '.version')}
                />
                <TimestampColumnElement parentName={props.parentName} />
              </>
            ))
            .with('follow', () => (
              <>
                <VersionElement
                  parentName={props.parentName}
                  label='Start version'
                  inputProps={{
                    'data-testid': 'input-version'
                  }}
                  onInput={unregisterConflicting(props.parentName + '.datetime')}
                ></VersionElement>
                <DatetimeElement
                  parentName={props.parentName}
                  label='Start datetime'
                  inputProps={{
                    'data-testid': 'input-datetime'
                  }}
                  onInput={unregisterConflicting(props.parentName + '.version')}
                />
              </>
            ))
            .with('snapshot_and_follow', () => (
              <>
                <FilterElement parentName={props.parentName} />
                <VersionElement
                  parentName={props.parentName}
                  label='Start version'
                  inputProps={{
                    'data-testid': 'input-version'
                  }}
                  onInput={unregisterConflicting(props.parentName + '.datetime')}
                ></VersionElement>
                <DatetimeElement
                  parentName={props.parentName}
                  label='Start datetime'
                  inputProps={{
                    'data-testid': 'input-datetime'
                  }}
                  onInput={unregisterConflicting(props.parentName + '.version')}
                />
                <TimestampColumnElement parentName={props.parentName} />
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
  ...props
}: { parentName: string } & Omit<BaseTextFieldProps, 'component'> & BigNumberInputProps) => (
  <FormTooltip
    title={
      '\
Optional table version number.\n\n\
When this option is set, the connector finds and opens the specified version of the table. \
In `snapshot` and `snapshot_and_follow` modes, it retrieves the snapshot of this version of \
the table.  In `follow` and `snapshot_and_follow` modes, it follows transaction log records \
**after** this version.\n\n\
Note: at most one of `version` and `datetime` options can be specified. \
When neither of the two options is specified, the latest committed version of the table \
is used.'
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
  </FormTooltip>
)
const DatetimeElement = (props: { parentName: string } & TextFieldProps) => (
  <FormTooltip
    title={
      '\
Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format.\n\n\
When this option is set, the connector finds and opens the version of the table as of the \
specified point in time.  In `snapshot` and `snapshot_and_follow` modes, it retrieves the \
snapshot of this version of the table.  In `follow` and `snapshot_and_follow` modes, it \
follows transaction log records **after** this version.\n\n\
Note: at most one of `version` and `datetime` options can be specified. \
When neither of the two options is specified, the latest committed version of the table \
is used.'
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
  </FormTooltip>
)

const FilterElement = (props: { parentName: string }) => (
  <FormTooltip
    title={
      "\
Optional row filter.\n\n\
This option is only valid when `mode` is set to `snapshot` or `snapshot_and_follow`.\n\n\
When specified, only rows that satisfy the filter condition are included in the \
snapshot.  The condition must be a valid SQL Boolean expression that can be used in \
the `where` clause of the `select * from snapshot where ...` query.\n\n\
This option can be used to specify the range of event times to include in the snapshot, \
e.g.: `ts BETWEEN '2005-01-01 00:00:00' AND '2010-12-31 23:59:59'`."
    }
  >
    <TextareaAutosizeElement
      resizeStyle='vertical'
      name={props.parentName + '.snapshot_filter'}
      label='Snapshot filter'
      size='small'
      fullWidth
      inputProps={{
        'data-testid': 'input-snapshot-filter'
      }}
      helperText='Boolean SQL expression'
      {...props}
    ></TextareaAutosizeElement>
  </FormTooltip>
)

const TimestampColumnElement = (props: { parentName: string }) => {
  return (
    <FormTooltip
      title={
        '\
Table column that serves as an event timestamp.\n\n\
When this option is specified, and `mode` is one of `snapshot` or `snapshot_and_follow`, \
the snapshot of the table will be sorted by the corresponding column.'
      }
    >
      <TextFieldElement
        name={props.parentName + '.timestamp_column'}
        label='Timestamp column'
        size='small'
        fullWidth
        inputProps={{
          'data-testid': 'input-timestamp-column'
        }}
      />
    </FormTooltip>
  )
}
