import 'dayjs/locale/en-gb'
import 'react-perfect-scrollbar/dist/css/styles.css'
import 'src/styles/globals.css'

/** @jsxImportSource react */
import themeConfig from '$lib/functions/configs/themeConfig'
import { ReactNode, Suspense } from 'react'

export const metadata = {
  description: `${themeConfig.templateName} – Web Console.`,
  keywords: 'Feldera Continuous Analytics Platform'
}

export const viewport = {
  width: 'device-width',
  initialScale: 1
}

export default (props: { children: ReactNode }) => {
  return (
    <html lang='en'>
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
      {/* <Suspense> resolves the warning of
        https://nextjs.org/docs/messages/deopted-into-client-rendering
        caused by the next layout in the hierarchy being Client Component */}
      <Suspense>{props.children}</Suspense>
    </html>
  )
}
