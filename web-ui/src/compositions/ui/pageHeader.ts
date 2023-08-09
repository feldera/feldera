import type { ReactNode } from 'react'
import { PageHeaderProps } from 'src/layouts/components/page-header/types'
import { create } from 'zustand'

type PageTitleStore = {
  header: PageHeaderProps
  setHeader: (title: PageHeaderProps) => void
}

export const usePageHeader = create<PageTitleStore>()(set => ({
  header: { title: null },
  setHeader: (header: PageHeaderProps) => set(() => ({ header }))
}))
