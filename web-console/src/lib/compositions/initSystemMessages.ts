export type SystemMessage = {
  id: string
  text: string
  action?: {
    text: string
    href: string
  }
  dismissable: 'never' | 'once' | 'session' | { forMs: number }
}

export const initSystemMessages: SystemMessage[] = []
