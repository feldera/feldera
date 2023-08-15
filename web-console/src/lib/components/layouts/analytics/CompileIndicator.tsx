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

export interface CompileIndicatorProps {
  id?: string
  state: ProgramStatus
}

export const CompileIndicator = (props: CompileIndicatorProps) => {
  const successLabel = 'Success'
  const compilingLabel = 'Compiling ...'

  const loadingIcon = <CircularProgress color='inherit' size='1rem' />
  const doneIcon = <DoneIcon />
  const errIcon = <ErrorOutlineIcon />

  interface ButtonState {
    color: ThemeColor
    isCompiling: boolean
    label: string
    toolTip?: string
    visible: boolean
  }

  const buttonState: ButtonState = match(props.state)
    .with({ SqlError: P.select() }, errs => ({
      visible: true,
      color: 'error' as const,
      isCompiling: false,
      label: errs.length + ' Compiler Error',
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
      label: successLabel
    }))
    .with({ SystemError: P._ }, () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: successLabel
    }))
    .with('None', () => ({ visible: false, color: 'warning' as const, isCompiling: false, label: compilingLabel }))
    .with('Pending', () => ({ visible: true, color: 'warning' as const, isCompiling: true, label: compilingLabel }))
    .with('CompilingSql', () => ({
      visible: true,
      color: 'warning' as const,
      isCompiling: true,
      label: compilingLabel
    }))
    .with('CompilingRust', () => ({
      visible: true,
      color: 'success' as const,
      isCompiling: false,
      label: successLabel
    }))
    // If you change the 'Success' string, adjust the webui-tester too
    .with('Success', () => ({ visible: true, color: 'success' as const, isCompiling: false, label: successLabel }))
    .exhaustive()

  if (buttonState.visible) {
    return (
      <Tooltip title={buttonState.toolTip}>
        <CustomChip
          id={props.id}
          sx={{ mr: 1 }}
          label={buttonState.label}
          skin='light'
          color={buttonState.color}
          icon={buttonState.isCompiling ? loadingIcon : buttonState.color == 'error' ? errIcon : doneIcon}
        />
      </Tooltip>
    )
  } else {
    return <></>
  }
}

export default CompileIndicator
