// The Compile indicator for the editor page.
//
// Changes color depending on compilation status and displays a tooltip with
// more information.

import { ProgramDescr } from '$lib/services/manager'
import { match, P } from 'ts-pattern'

import CustomChip from '@core/components/mui/chip'
import { ThemeColor } from '@core/layouts/types'
import DoneIcon from '@mui/icons-material/Done'
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline'
import { CircularProgress, Tooltip } from '@mui/material'

export const CompileIndicator = (props: { program: ProgramDescr }) => {
  const labelSuccess = 'Success'
  const labelCompiling = 'Compiling …'
  const labelPending = 'In queue'

  const loadingIcon = <CircularProgress data-testid='box-spinner' color='inherit' size='1rem' />
  const doneIcon = <DoneIcon />
  const errIcon = <ErrorOutlineIcon />

  type ButtonState =
    | {
        color: ThemeColor
        isCompiling: boolean
        label: string
        status: string
        toolTip?: string
        visible: boolean
      }
    | { visible: false }

  const buttonState: ButtonState = match(props.program)
    .with({ status: { SqlError: P.select() } }, errs => ({
      visible: true,
      color: 'error' as const,
      isCompiling: false,
      label: errs.length + ' Compiler Error',
      status: 'compilerError',
      toolTip:
        'Compilation had ' +
        errs.length +
        ' ' +
        (errs.length > 1 ? 'errors' : 'error') +
        ', check highlighted lines in the editor for more details.'
    }))
    .with({ status: { RustError: P._ } }, () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: labelSuccess,
      status: 'success'
    }))
    .with({ status: { SystemError: P._ } }, () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: labelSuccess,
      status: 'success'
    }))
    .with({ version: 0 }, () => ({
      visible: false as const
    }))
    .with({ status: 'Pending' }, () => ({
      visible: true,
      color: 'warning' as const,
      isCompiling: true,
      label: labelPending,
      status: 'queued'
    }))
    .with({ status: 'CompilingSql' }, () => ({
      visible: true,
      color: 'warning' as const,
      isCompiling: true,
      label: labelCompiling,
      status: 'compiling'
    }))
    .with({ status: 'CompilingRust' }, () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: labelSuccess,
      status: 'success'
    }))
    .with({ status: 'Success' }, () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: labelSuccess,
      status: 'success'
    }))
    .exhaustive()

  if (!buttonState.visible) {
    return <></>
  }
  return (
    <Tooltip title={buttonState.toolTip}>
      <CustomChip
        sx={{ mr: 1 }}
        label={buttonState.label}
        skin='light'
        color={buttonState.color}
        icon={buttonState.isCompiling ? loadingIcon : buttonState.color == 'error' ? errIcon : doneIcon}
        data-testid={'box-compile-status-' + buttonState.status}
      />
    </Tooltip>
  )
}

export default CompileIndicator
