// State to remember if the drawer to select a connector is open or not.

import { create } from 'zustand'

interface DrawerState {
  isOpen: boolean
  forNodes: 'inputNode' | 'outputNode'
  close: () => void
  open: (forNodes: 'inputNode' | 'outputNode') => void
}

const useDrawerState = create<DrawerState>()(set => ({
  isOpen: false,
  forNodes: 'inputNode',
  close: () => set(() => ({ isOpen: false })),
  open: (forNodes: 'inputNode' | 'outputNode') => {
    set({ isOpen: true, forNodes })
  }
}))

export default useDrawerState
