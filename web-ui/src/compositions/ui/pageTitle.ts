import type { ReactNode } from 'react';
import { create } from 'zustand'

type PageTitleStore = {
   header: ReactNode
   setHeader: (title: ReactNode) => void
}

export const usePageHeader = create<PageTitleStore>()((set) => ({
   header: null,
   setHeader: (header: ReactNode) => set(() => ({ header })),
}))

