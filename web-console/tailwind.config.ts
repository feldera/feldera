import { join } from 'path'

import { skeleton } from '@skeletonlabs/skeleton/plugin'
import forms from '@tailwindcss/forms'

import { felderaTheme } from './src/felderaTheme'

import type { Config } from 'tailwindcss'

const config = {
  darkMode: 'selector',
  content: [
    './src/**/*.{html,js,svelte,ts}',
    join(require.resolve('@skeletonlabs/skeleton-svelte'), '../**/*.{html,js,svelte,ts}')
  ],
  theme: {
    extend: {
      fontFamily: {
        brands: '"Font Awesome 6 Brands"'
      }
    }
  },
  plugins: [
    forms,
    skeleton({
      themes: [felderaTheme]
    })
  ]
} satisfies Config

export default config
