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
            <Button onClick={closeDialog} autoFocus data-testid='button-global-dialog-cancel'>
              Cancel
            </Button>
            <Button
              onClick={() => {
                content.onSuccess.callback()
                closeDialog()
              }}
              data-testid={content.onSuccess['data-testid']}
            >
              {content.onSuccess.name}
            </Button>
          </DialogActions>
        </>
      )}
    </Dialog>
  )
}
