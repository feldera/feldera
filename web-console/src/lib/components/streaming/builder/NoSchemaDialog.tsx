import { useRouter } from 'next/navigation'
import { Dispatch, SetStateAction } from 'react'

import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogContentText from '@mui/material/DialogContentText'
import DialogTitle from '@mui/material/DialogTitle'

const MissingSchemaDialog = (props: {
  open: boolean
  setOpen: Dispatch<SetStateAction<boolean>>
  program_name: string | undefined
}) => {
  const router = useRouter()
  const handleClose = () => {
    props.setOpen(false)
    router.push(`/analytics/editor/?program_name=${props.program_name}`)
  }

  return (
    <Dialog
      open={props.open && props.program_name !== undefined}
      disableEscapeKeyDown
      aria-labelledby='alert-dialog-title'
      aria-describedby='alert-dialog-description'
      onClose={(event, reason) => {
        if (reason !== 'backdropClick') {
          handleClose()
        }
      }}
    >
      <DialogTitle>Missing Schema?</DialogTitle>
      <DialogContent>
        <DialogContentText>
          We didn't find the schema for the program of the config you are trying to load. Either the program has not
          been compiled yet or the compilation had errors. Continue to the program editor to resolve the issue.
        </DialogContentText>
      </DialogContent>
      <DialogActions>
        <Button variant='contained' onClick={handleClose}>
          Edit Program
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export default MissingSchemaDialog
