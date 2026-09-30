import Colorizr from 'colorizr'

export const getThemeColor = (color: string) => {
  try {
    return new Colorizr(
      getComputedStyle(document.body).getPropertyValue(color).trim().replace(/deg/, '')
    )
  } catch (e) {
    console.error('getThemeColor error:', e)
    return undefined!
  }
}
