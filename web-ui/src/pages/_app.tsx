import Head from 'next/head'
import { Router } from 'next/router'
import type { NextPage } from 'next'
import type { AppProps } from 'next/app'
import NProgress from 'nprogress'
import { CacheProvider } from '@emotion/react'
import type { EmotionCache } from '@emotion/cache'
import themeConfig from 'src/configs/themeConfig'
import StandardVerticalLayout from 'src/layouts/StandardVerticalLayout'
import ThemeComponent from 'src/@core/theme/ThemeComponent'
import { SettingsConsumer, SettingsProvider } from 'src/@core/context/settingsContext'
import { createEmotionCache } from 'src/@core/utils/create-emotion-cache'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs'
import { LocalizationProvider } from '@mui/x-date-pickers'
import { LicenseInfo } from '@mui/x-license-pro'
import 'dayjs/locale/en-gb'

import 'react-perfect-scrollbar/dist/css/styles.css'
import '../../styles/globals.css'
import StatusSnackBar from 'src/components/errors/StatusSnackBar'
import { defaultQueryFn } from 'src/types/defaultQueryFn'
import { OpenAPI } from 'src/types/manager'

type ExtendedAppProps = AppProps & {
  Component: NextPage
  emotionCache: EmotionCache
}

LicenseInfo.setLicenseKey(process.env.NEXT_PUBLIC_MUIX_PRO_KEY || 'unset')
OpenAPI.BASE =
  (typeof window !== 'undefined' && window.location.origin
    ? window.location.origin.endsWith(':3000')
      ? // If we're running locally with `yarn dev` on port 3000, we point to the
        // backend server running on port 8080
        window.location.origin.replace(':3000', ':8080')
      : // Otherwise the API and UI URL will be the same
        window.location.origin
    : '') + '/v0'

const clientSideEmotionCache = createEmotionCache()

if (themeConfig.routingLoader) {
  Router.events.on('routeChangeStart', () => {
    NProgress.start()
  })
  Router.events.on('routeChangeError', () => {
    NProgress.done()
  })
  Router.events.on('routeChangeComplete', () => {
    NProgress.done()
  })
}
// provide the default query function to your app with defaultOptions
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      queryFn: defaultQueryFn
    }
  }
})

const App = (props: ExtendedAppProps) => {
  const { Component, emotionCache = clientSideEmotionCache, pageProps } = props

  // Variables
  const getLayout = Component.getLayout ?? (page => <StandardVerticalLayout>{page}</StandardVerticalLayout>)

  return (
    <CacheProvider value={emotionCache}>
      <Head>
        <title>{`${themeConfig.templateName} - WebUI`}</title>
        <meta name='description' content={`${themeConfig.templateName} – WebUI.`} />
        <meta name='keywords' content='Database Stream Processor Configuration UI' />
        <meta name='viewport' content='initial-scale=1, width=device-width' />
      </Head>

      <SettingsProvider>
        <SettingsConsumer>
          {({ settings }) => {
            return (
              <LocalizationProvider dateAdapter={AdapterDayjs} adapterLocale='en-gb'>
                <ThemeComponent settings={settings}>
                  <QueryClientProvider client={queryClient}>
                    {getLayout(
                      <>
                        <Component {...pageProps} />
                        <StatusSnackBar />
                      </>
                    )}
                  </QueryClientProvider>
                </ThemeComponent>
              </LocalizationProvider>
            )
          }}
        </SettingsConsumer>
      </SettingsProvider>
    </CacheProvider>
  )
}

export default App
