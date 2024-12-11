import { join } from 'path'

import { skeleton } from '@skeletonlabs/skeleton/plugin'
import scrollbar from 'tailwind-scrollbar'
import forms from '@tailwindcss/forms'

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
      },
      colors: {
        dark: 'rgb(24 24 24)'
      }
    },
    screens: {
      sm: '640px',
      // => @media (min-width: 640px) { ... }
      md: '768px',
      // => @media (min-width: 768px) { ... }
      lg: '1024px',
      // => @media (min-width: 1024px) { ... }
      xl: '1280px',
      // => @media (min-width: 1280px) { ... }
      '2xl': '1536px'
      // => @media (min-width: 1536px) { ... }
    }
  },
  plugins: [
    forms,
    skeleton({
      themes: [felderaModern]
    }),
    scrollbar({ nocompatible: true })
  ]
} satisfies Config

export default config
