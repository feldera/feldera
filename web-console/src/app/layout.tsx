import 'dayjs/locale/en-gb'
import 'react-perfect-scrollbar/dist/css/styles.css'
import 'src/styles/globals.css'

/** @jsxImportSource react */
import themeConfig from '$lib/functions/configs/themeConfig'
import { Public_Sans } from 'next/font/google'
import { ReactNode } from 'react'

const publicSans = Public_Sans({
  weight: ['300', '400', '500', '600', '700'],
  style: ['normal', 'italic'],
  subsets: ['latin']
})

export const metadata = {
  description: `${themeConfig.templateName} – Web Console.`,
  keywords: 'Feldera Continuous Analytics Platform',
  viewport: 'initial-scale=1, width=device-width'
}

export default (props: { children: ReactNode }) => {
  return (
    <html lang='en' className={publicSans.className}>
      <head>
        <title>{`${themeConfig.templateName} - Web Console`}</title>
        {false && (process.env.NODE_ENV === 'development' || process.env.VERCEL_ENV === 'preview') && (
          // eslint-disable-next-line @next/next/no-sync-scripts
          <script
            data-project-id='rC1VPOsRB4m9SZb3zOUAS91NJ4CjskhMwplh2SOr'
            src='https://snippet.meticulous.ai/v1/meticulous.js'
          />
        )}
        <link rel='preconnect' href='https://fonts.googleapis.com' />
        <link rel='preconnect' href='https://fonts.gstatic.com' />
        <link rel='apple-touch-icon' sizes='180x180' href='/favicon.svg' />
        <link rel='shortcut icon' href='/favicon.svg' />
      </head>
      <body>{props.children}</body>
    </html>
  )
}
