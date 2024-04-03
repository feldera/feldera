import { Row } from '$lib/functions/sqlValue'
import { useState } from 'react'

import DeleteIcon from '@mui/icons-material/Delete'
import { Button, ButtonProps, Dialog, DialogActions, DialogTitle } from '@mui/material'
import { GridRowId, useGridApiContext } from '@mui/x-data-grid-pro'

export const RowDeleteButton = ({
  onDeleteRows,
  ...props
}: ButtonProps & { onDeleteRows: (rows: Map<GridRowId, Row>) => void }) => {
  const { current: ref } = useGridApiContext()
  const rows = ref.getSelectedRows() as Map<GridRowId, Row>
  const [dialogOpen, setDialogOpen] = useState(false)

  return (
    <>
      {rows.size > 0 && (
        <Button
          {...props}
          variant='text'
          startIcon={<DeleteIcon />}
          size='small'
          onClick={setDialogOpen.bind(null, true)}
        >
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
              onDeleteRows(rows)
            }}
          >
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </>
  )
}
