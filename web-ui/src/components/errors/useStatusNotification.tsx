// Global state management and hooks for status notifications.
//
// e.g., the small popups in the bottom left corner that go away after some
// time. Mostly used for errors.

import { AlertColor } from '@mui/material/Alert'
import { create } from 'zustand'

export interface StatusSnackBarMessage {
  key: number
  message: string
  color: AlertColor
}

interface StatusNotificationState {
  isOpen: boolean
  statusMessages: StatusSnackBarMessage[]
  messageInfo: StatusSnackBarMessage | undefined
  pushMessage: (m: StatusSnackBarMessage) => void
  popMessage: () => void
  setMessageInfo(m: StatusSnackBarMessage | undefined): void
  setOpen(open: boolean): void
}

const useStatusNotification = create<StatusNotificationState>()(set => ({
  isOpen: false,
  statusMessages: [],
  messageInfo: undefined,
  pushMessage: (msg: StatusSnackBarMessage) => set(state => ({ statusMessages: [...state.statusMessages, msg] })),
  popMessage: () => set(state => ({ statusMessages: state.statusMessages.slice(1) })),
  setOpen: (open: boolean) => {
    set({ isOpen: open })
  },
  setMessageInfo: (messageInfo: StatusSnackBarMessage) => {
    set({ messageInfo })
  }
}))

export default useStatusNotification
