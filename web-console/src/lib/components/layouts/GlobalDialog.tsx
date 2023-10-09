import { useDeleteDialog } from '$lib/compositions/useDialog'

import { Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle } from '@mui/material'

export const GlobalDialog = () => {
  const { content, closeDialog } = useDeleteDialog()
  return (
    <Dialog onClose={closeDialog} open={!!content}>
      {content && (
        <>
          <DialogTitle>{content.title}</DialogTitle>
          {!!content.description && (
            <DialogContent>
              <DialogContentText>{content.description}</DialogContentText>
            </DialogContent>
          )}
          <DialogActions>
            <Button onClick={closeDialog} autoFocus>
              Cancel
            </Button>
            <Button
              onClick={() => {
                content.onSuccess.callback()
                closeDialog()
              }}
            >
              {content.onSuccess.name}
            </Button>
          </DialogActions>
        </>
      )}
    </Dialog>
  )
}
