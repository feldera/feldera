/**
 * Applies the document-root state that the real app sets outside any component,
 * so browser tests render with the production theme rather than browser defaults.
 *
 * Two attributes carry the whole design system:
 *   - `data-theme` (src/app.html) scopes every feldera-theme custom property,
 *     including `--base-font-family: DM Sans Variable`. Without it Skeleton's
 *     `body { font-family: var(--base-font-family) }` resolves to nothing and the
 *     page falls back to system fonts and default colors.
 *   - the `light`/`dark` class (src/routes/+layout.svelte) drives the `dark`
 *     custom variant declared in src/routes/layout.css.
 */

export type ColorScheme = 'light' | 'dark'

export const applyAppShell = (mode: ColorScheme = 'light') => {
  document.documentElement.dataset.theme = 'feldera-modern-theme'
  document.documentElement.classList.remove('light', 'dark')
  document.documentElement.classList.add(mode)
  document.body.classList.add('h-screen')
}

/**
 * Resolves once webfonts have finished loading. Screenshots taken before this
 * capture fallback glyphs, because `@font-face` in src/assets/fonts/dm-sans.css
 * uses `font-display: block` and loads asynchronously.
 */
export const waitForFonts = async () => {
  await document.fonts.load('16px "DM Sans Variable"')
  await document.fonts.ready
}
