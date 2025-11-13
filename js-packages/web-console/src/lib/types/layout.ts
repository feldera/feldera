export type NavLink = {
  path?: string | string[]
  title: string
  action?: string
  subject?: string
  disabled?: boolean
  badgeContent?: string
  externalLink?: boolean
  openInNewTab?: boolean
  class?: string
  badgeColor?: 'default' | 'primary' | 'secondary' | 'success' | 'error' | 'warning' | 'info'
  testid?: string
}

export type NavSectionTitle = {
  sectionTitle: string
  action?: string
  subject?: string
}

export type NavItem = NavLink | NavSectionTitle
