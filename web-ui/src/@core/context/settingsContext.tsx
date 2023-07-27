import { createContext, ReactNode } from 'react'
import { PaletteMode } from '@mui/material'
import themeConfig from 'src/configs/themeConfig'
import { ThemeColor, ContentWidth } from 'src/@core/layouts/types'
import { useLocalStorage } from '@mantine/hooks'
import { LS_PREFIX } from 'src/types/localStorage'

export type Settings = {
  mode: PaletteMode
  themeColor: ThemeColor
  contentWidth: ContentWidth
}

export type SettingsContextValue = {
  settings: Settings
  saveSettings: (updatedSettings: Settings) => void
}

const initialSettings: Settings = {
  themeColor: 'primary',
  mode: themeConfig.mode,
  contentWidth: themeConfig.contentWidth
}

export const SettingsContext = createContext<SettingsContextValue>({
  saveSettings: () => null,
  settings: initialSettings
})

export const SettingsProvider = ({ children }: { children: ReactNode }) => {
  const [settings, setSettings] = useLocalStorage({
    key: LS_PREFIX + 'theme-settings',
    defaultValue: initialSettings
  })

  const saveSettings = (updatedSettings: Settings) => {
    setSettings(updatedSettings)
  }

  return <SettingsContext.Provider value={{ settings, saveSettings }}>{children}</SettingsContext.Provider>
}

export const SettingsConsumer = SettingsContext.Consumer
