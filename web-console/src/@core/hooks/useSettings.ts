import { useContext } from 'react'

import { SettingsContext, SettingsContextValue } from '@core/context/settingsContext'

export const useSettings = (): SettingsContextValue => useContext(SettingsContext)
