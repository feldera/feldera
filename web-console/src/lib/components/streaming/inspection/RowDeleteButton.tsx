import { useState } from 'react'

import DeleteIcon from '@mui/icons-material/Delete'
import { Button, Dialog, DialogActions, DialogTitle } from '@mui/material'
import { GridRowId, GridValidRowModel, useGridApiContext } from '@mui/x-data-grid-pro'

export const RowDeleteButton = (props: { onDeleteRows: (rows: Map<GridRowId, GridValidRowModel>) => void }) => {
  const { current: ref } = useGridApiContext()
  const rows = ref.getSelectedRows()
  const [dialogOpen, setDialogOpen] = useState(false)

  return (
    <>
      {rows.size > 0 && (
        <Button variant='text' startIcon={<DeleteIcon />} size='small' onClick={setDialogOpen.bind(null, true)}>
          Delete
        </Button>
      )}
      <Dialog onClose={setDialogOpen.bind(null, false)} open={dialogOpen}>
        <DialogTitle>
          Delete {rows.size} {rows.size > 1 ? 'rows' : 'row'}?
        </DialogTitle>
        <DialogActions>
          <Button onClick={setDialogOpen.bind(null, false)}>Cancel</Button>
          <Button
            onClick={() => {
              setDialogOpen.bind(null, false)()
              props.onDeleteRows(rows)
            }}
          >
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </>
  )
}
