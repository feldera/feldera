export type SystemMessage = {
  id: string
  text: string
  action?: {
    text: string
    href: string
  }
  dismissable: 'never' | 'once' | 'session' | { forMs: number } // Message can be dismissed for N ms, after which it will be displayed again
  onDismiss?: () => void
}

export const initSystemMessages: SystemMessage[] = []
