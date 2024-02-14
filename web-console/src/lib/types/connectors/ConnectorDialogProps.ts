import { ConnectorDescr } from '$lib/services/manager'
import { Dispatch, SetStateAction } from 'react'

/**
 * The properties passed to a create/update connector dialog.
 */
export type ConnectorDialogProps = {
  connector?: ConnectorDescr
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  onSuccess?: (connector: ConnectorDescr, oldConnectorName: string) => void
  existingTitle: ((name: string) => string) | null
  submitButton: JSX.Element
  disabled?: boolean
}
