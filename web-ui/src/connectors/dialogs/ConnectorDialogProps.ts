// The properties passed to a create/update connector dialog.

import { Dispatch, SetStateAction } from 'react'
import { ConnectorDescr, ConnectorId } from 'src/types/manager'

interface ConnectorDialogProps {
  connector?: ConnectorDescr
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  onSuccess?: Dispatch<ConnectorId>
}

export default ConnectorDialogProps
