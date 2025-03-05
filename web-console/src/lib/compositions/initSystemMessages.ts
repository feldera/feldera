export type SystemMessage = {
  id: string
  text: string
  action?: {
    text: string
    href: string
  }
  dismissable: 'never' | 'once' | 'session' | { milliseconds: number }
}

export const initSystemMessages: SystemMessage[] = []
