// The Compile indicator for the editor page.
//
// Changes color depending on compilation status and displays a tooltip with
// more information.

import { ProgramStatus } from '$lib/services/manager'
import CustomChip from 'src/@core/components/mui/chip'
import { ThemeColor } from 'src/@core/layouts/types'
import { match, P } from 'ts-pattern'

import DoneIcon from '@mui/icons-material/Done'
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline'
import { CircularProgress, Tooltip } from '@mui/material'

export const CompileIndicator = (props: { state: ProgramStatus }) => {
  const labelSuccess = 'Success'
  const labelCompiling = 'Compiling …'
  const labelPending = 'In queue'

  const loadingIcon = <CircularProgress data-testid='box-spinner' color='inherit' size='1rem' />
  const doneIcon = <DoneIcon />
  const errIcon = <ErrorOutlineIcon />

  interface ButtonState {
    color: ThemeColor
    isCompiling: boolean
    label: string
    status: string
    toolTip?: string
    visible: boolean
  }

  const buttonState: ButtonState = match(props.state)
    .with({ SqlError: P.select() }, errs => ({
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
    .with({ RustError: P._ }, () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: labelSuccess,
      status: 'success'
    }))
    .with({ SystemError: P._ }, () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: labelSuccess,
      status: 'success'
    }))
    .with('Pending', () => ({
      visible: true,
      color: 'warning' as const,
      isCompiling: true,
      label: labelPending,
      status: 'queued'
    }))
    .with('CompilingSql', () => ({
      visible: true,
      color: 'warning' as const,
      isCompiling: true,
      label: labelCompiling,
      status: 'compiling'
    }))
    .with('CompilingRust', () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: labelSuccess,
      status: 'success'
    }))
    .with('Success', () => ({
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
