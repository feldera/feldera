'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import { GridItems } from '$lib/components/common/GridItems'
import { DemoCleanupDialog } from '$lib/components/demo/DemoCleanupDialog'
import { DemoSetupDialog } from '$lib/components/demo/DemoSetupDialog'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { useRouter } from 'next/navigation'
import { Suspense, use } from 'react'
import { useHashPart } from 'src/lib/compositions/useHashPart'
import { DemoSetup } from 'src/lib/types/demo'
import { match } from 'ts-pattern'

import { Box, Button, Card, CardActions, CardContent, Grid, IconButton, Typography } from '@mui/material'
import { useQuery } from '@tanstack/react-query'

const DemoTile = (props: { name: string; desc: string; onSetup: () => void; onCleanup: () => void }) => {
  return (
    <Card>
      <CardContent>
        <Typography variant='h5' gutterBottom>
          {props.name}
        </Typography>
        <Typography variant='body1'>{props.desc}</Typography>
      </CardContent>
      <CardActions sx={{ width: '100%', display: 'flex', justifyContent: 'space-between' }}>
        <IconButton onClick={props.onCleanup}>
          <i className={`bx bx-trash-alt`} style={{ fontSize: 24 }} />
        </IconButton>
        <Button
          onClick={props.onSetup}
          variant='contained'
          sx={{ px: '1rem' }}
          endIcon={<i className={`bx bx-chevron-right`} style={{}} />}
        >
          Try
        </Button>
      </CardActions>
    </Card>
  )
}

const _fetchStaticDemoSetup = (demo: { import: string }) =>
  fetch(`/_next/static/demo/${demo.import}`).then(r => r.json()) as Promise<DemoSetup>

const fetchDemoSetup = (demo: { url: string }) => fetch(demo.url).then(r => r.json()) as Promise<DemoSetup>

const fetchDemoByTitle = async (
  demos: {
    url: string
    title: any
    description: any
  }[],
  title: string
) => {
  const demo = demos.find(demo => demo.title === title)
  if (!demo) {
    return undefined
  }
  const setup = await fetchDemoSetup(demo)
  return {
    name: title,
    setup
  }
}

const DemoActionDialogs = (props: {
  demos: {
    url: string
    title: any
    description: any
  }[]
}) => {
  const [hash, setWantedDemoAction] = useHashPart()
  const wantedDemoAction = decodeURI(hash)
  const action = /^(\w+)\//.exec(wantedDemoAction)?.[1] as 'setup' | 'cleanup' | undefined
  const demoTitle = /\/([\w ]+)$/.exec(wantedDemoAction)?.[1]

  if (!demoTitle || !action) {
    return <></>
  }
  const demo = use(fetchDemoByTitle(props.demos, demoTitle))
  return match(action)
    .with('setup', () => <DemoSetupDialog demo={demo} onClose={() => setWantedDemoAction('')}></DemoSetupDialog>)
    .with('cleanup', () => <DemoCleanupDialog demo={demo} onClose={() => setWantedDemoAction('')}></DemoCleanupDialog>)
    .exhaustive()
}

export default function () {
  const queryDemos = useQuery(usePipelineManagerQuery().getDemos())

  const demos = queryDemos.data ?? []
  const router = useRouter()
  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/demos/`} data-testid='button-breadcrumb-demos'>
          Demos
        </Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <Box>
        <Typography variant='body1' sx={{ mb: '2rem' }}>
          Setup and explore pre-made demos on this Feldera instance
        </Typography>
        <Grid container spacing={2}>
          <GridItems xs={6} sm={4} md={3}>
            {demos.map(demo => (
              <DemoTile
                key={demo.title}
                name={demo.title}
                desc={demo.description}
                onSetup={() => router.push('#setup/' + demo.title)}
                onCleanup={() => router.push('#cleanup/' + demo.title)}
              ></DemoTile>
            ))}
          </GridItems>
        </Grid>
      </Box>
      <Suspense>
        <DemoActionDialogs demos={demos}></DemoActionDialogs>
      </Suspense>
    </>
  )
}
