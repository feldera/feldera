import React from 'react'
import clsx from 'clsx'
import useDocusaurusContext from '@docusaurus/useDocusaurusContext'
import Layout from '@theme/Layout'
import HomepageFeatures from '@site/src/components/HomepageFeatures'

import Typed from '@site/src/theme/Typed'
import styles from './index.module.css'
import { FlowingLines } from '../components/HomepageShader'

function HomepageHeader(): JSX.Element {
  const { siteConfig } = useDocusaurusContext()

  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className='row'>
        <div className='col col--2'></div>
        <div className='col col--6'>
          <div className='row'>
            <div className='col col--12'>
              <h1 className='hero__title'>{siteConfig.title}</h1>
              <p className='hero__subtitle'>
                <Typed strings={[siteConfig.tagline]} typeSpeed={75} />
              </p>
            </div>
          </div>
        </div>
        <div className='col col--12'>
          <FlowingLines />
        </div>
      </div>
    </header>
  )
}

export default function Home(): JSX.Element {
  const { siteConfig } = useDocusaurusContext()
  return (
    <Layout title={`DBSP`} description='Streaming data processing engine'>
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  )
}
