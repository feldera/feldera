'use client'

import themeConfig from '$lib/functions/configs/themeConfig'
import { LS_PREFIX } from '$lib/types/localStorage'
import { createContext, ReactNode } from 'react'

import { Settings } from '@core/context/settingsTypes'
import { useLocalStorage } from '@mantine/hooks'

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
