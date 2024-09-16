import { join } from 'path'

import { skeleton } from '@skeletonlabs/skeleton/plugin'
import forms from '@tailwindcss/forms'

import { felderaClassic } from './src/lib/functions/themes/feldera-classic'

import { felderaModern } from './src/lib/functions/themes/feldera-modern'

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
        brands: '"Font Awesome 6 Brands"',
        'dm-mono': '"DM Mono"'
      }
    }
  },
  plugins: [
    forms,
    skeleton({
      themes: [felderaClassic, felderaModern]
    })
  ]
} satisfies Config

export default config
