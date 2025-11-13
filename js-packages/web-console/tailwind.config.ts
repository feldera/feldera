import { join } from 'path'

import { skeleton } from '@skeletonlabs/skeleton/plugin'
import scrollbar from 'tailwind-scrollbar'
import forms from '@tailwindcss/forms'

import { felderaModern } from './src/lib/functions/themes/feldera-modern'

import type { Config } from 'tailwindcss'

const baseFontSize = 14
const makeRemSize = (px: number) => `${px / baseFontSize}rem /* ${px}px */`

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
      },
      fontSize: {
        xs: [makeRemSize(10), makeRemSize(14)],
        sm: [makeRemSize(12), makeRemSize(18)],
        base: [makeRemSize(14), makeRemSize(20)],
        lg: [makeRemSize(16), makeRemSize(22)],
        xl: [makeRemSize(20), makeRemSize(28)],
        '2xl': [makeRemSize(24), makeRemSize(28)],
        '3xl': [makeRemSize(30), makeRemSize(36)],
        '4xl': [makeRemSize(36), makeRemSize(40)],
        '5xl': [makeRemSize(48), `1`],
        '6xl': [makeRemSize(60), `1`],
        '7xl': [makeRemSize(72), `1`],
        '8xl': [makeRemSize(96), `1`],
        '9xl': [makeRemSize(128), `1`]
      },
      spacing: {
        '0': '0px',
        px: '1px',
        '0.5': makeRemSize(2),
        '1': makeRemSize(4),
        '1.5': makeRemSize(6),
        '2': makeRemSize(8),
        '2.5': makeRemSize(10),
        '3': makeRemSize(12),
        '3.5': makeRemSize(14),
        '4': makeRemSize(16),
        '5': makeRemSize(20),
        '6': makeRemSize(24),
        '7': makeRemSize(28),
        '8': makeRemSize(32),
        '9': makeRemSize(36),
        '10': makeRemSize(40),
        '11': makeRemSize(44),
        '12': makeRemSize(48),
        '14': makeRemSize(56),
        '16': makeRemSize(64),
        '20': makeRemSize(80),
        '24': makeRemSize(96),
        '28': makeRemSize(112),
        '32': makeRemSize(128),
        '36': makeRemSize(144),
        '40': makeRemSize(160),
        '44': makeRemSize(176),
        '48': makeRemSize(192),
        '52': makeRemSize(208),
        '56': makeRemSize(224),
        '60': makeRemSize(240),
        '64': makeRemSize(256),
        '72': makeRemSize(288),
        '80': makeRemSize(320),
        '96': makeRemSize(384),
        '-px': '-1px',
        '-0.5': makeRemSize(-2),
        '-1': makeRemSize(-4),
        '-1.5': makeRemSize(-6),
        '-2': makeRemSize(-8),
        '-2.5': makeRemSize(-10),
        '-3': makeRemSize(-12),
        '-3.5': makeRemSize(-14),
        '-4': makeRemSize(-16),
        '-5': makeRemSize(-20),
        '-6': makeRemSize(-24),
        '-7': makeRemSize(-28),
        '-8': makeRemSize(-32),
        '-9': makeRemSize(-36),
        '-10': makeRemSize(-40),
        '-11': makeRemSize(-44),
        '-12': makeRemSize(-48),
        '-14': makeRemSize(-56),
        '-16': makeRemSize(-64),
        '-20': makeRemSize(-80),
        '-24': makeRemSize(-96),
        '-28': makeRemSize(-112),
        '-32': makeRemSize(-128),
        '-36': makeRemSize(-144),
        '-40': makeRemSize(-160),
        '-44': makeRemSize(-176),
        '-48': makeRemSize(-192),
        '-52': makeRemSize(-208),
        '-56': makeRemSize(-224),
        '-60': makeRemSize(-240),
        '-64': makeRemSize(-256),
        '-72': makeRemSize(-288),
        '-80': makeRemSize(-320),
        '-96': makeRemSize(-384)
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
