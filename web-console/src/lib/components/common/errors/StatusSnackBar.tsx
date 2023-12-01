'use client'

// Layout of the status snackbar component.
//
// The message can be clicked away or it will disappear after a few seconds.

import { SyntheticEvent, useEffect } from 'react'

import Alert, { AlertColor } from '@mui/material/Alert'
import Snackbar from '@mui/material/Snackbar'

import useStatusNotification from './useStatusNotification'

export interface StatusSnackBarMessage {
  key: number
  message: string
  color: AlertColor
}

const StatusSnackBar = () => {
  const { isOpen, setOpen, messageInfo, setMessageInfo, statusMessages, popMessage } = useStatusNotification()

  useEffect(() => {
    if (statusMessages.length && !messageInfo) {
      setOpen(true)
      popMessage()
      setMessageInfo({ ...statusMessages[0] })
    } else if (statusMessages.length && messageInfo && isOpen) {
      setOpen(false)
    }
  }, [statusMessages, messageInfo, isOpen, setOpen, setMessageInfo, popMessage])

  const handleClose = (event: Event | SyntheticEvent, reason?: string) => {
    if (reason === 'clickaway') {
      return
    }
    setOpen(false)
  }

  const handleExited = () => {
    setMessageInfo(undefined)
  }

  return (
    <Snackbar
      open={isOpen}
      onClose={handleClose}
      autoHideDuration={3000}
      TransitionProps={{ onExited: handleExited }}
      key={messageInfo ? messageInfo.key : undefined}
    >
      <Alert
        elevation={3}
        variant='filled'
        onClose={handleClose}
        severity={messageInfo?.color || 'success'}
        data-testid='box-snackbar-popup'
      >
        {messageInfo?.message}
      </Alert>
    </Snackbar>
  )
}

export default StatusSnackBar
