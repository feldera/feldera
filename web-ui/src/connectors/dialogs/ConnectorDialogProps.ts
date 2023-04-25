// The properties passed to a create/update connector dialog.

import { Dispatch, SetStateAction } from 'react'
import { ConnectorDescr } from 'src/types/manager'

interface ConnectorDialogProps {
  connector?: ConnectorDescr
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  onSuccess?: Dispatch<ConnectorDescr>
}

export default ConnectorDialogProps
