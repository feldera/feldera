'use client'

import publicDemos from '$demo/publicDemos.json'
import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import { GridItems } from '$lib/components/common/GridItems'
import { DemoCleanupDialog } from '$lib/components/demo/DemoCleanupDialog'
import { DemoSetupDialog } from '$lib/components/demo/DemoSetupDialog'
import { Fragment, useState } from 'react'
import { DemoSetup } from 'src/lib/types/demo'
import IconBrush from '~icons/bx/brush'
import IconChevronRight from '~icons/bx/chevron-right'

import { Box, Button, Card, CardActions, CardContent, Grid, IconButton, Link, Typography } from '@mui/material'

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
        <IconButton onClick={props.onCleanup} sx={{ transform: 'rotate(180deg)' }}>
          <IconBrush fontSize={20} />
        </IconButton>
        <Button onClick={props.onSetup} endIcon={<IconChevronRight />}>
          Try
        </Button>
      </CardActions>
    </Card>
  )
}

const fetchDemoSetup = (demo: { import: string }) =>
  import(`$demo/${demo.import}`).then(imported => imported.default as DemoSetup)

export default function () {
  const [setupDemo, setSetupDemo] = useState<{ name: string; setup: DemoSetup } | undefined>()
  const [cleanupDemo, setCleanupDemo] = useState<{ name: string; setup: DemoSetup } | undefined>()
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/lessons/`} data-testid='button-breadcrumb-lessons'>
          Feldera Lessons
        </Link>
      </BreadcrumbsHeader>
      <Box>
        <Typography variant='h4' gutterBottom>
          Demos
        </Typography>
        <Typography variant='body1' gutterBottom>
          Setup and explore pre-made demos on your running Feldera instance
        </Typography>
        {publicDemos.map(group => (
          <Fragment key={group.group}>
            <Typography variant='h6' gutterBottom>
              {group.label}
            </Typography>
            <Grid container spacing={2}>
              <GridItems xs={6} sm={4} md={3}>
                {group.demos.map(demo => (
                  <DemoTile
                    key={demo.name}
                    name={demo.name}
                    desc={demo.description}
                    onSetup={() => fetchDemoSetup(demo).then(setup => setSetupDemo({ name: demo.name, setup }))}
                    onCleanup={() => fetchDemoSetup(demo).then(setup => setCleanupDemo({ name: demo.name, setup }))}
                  ></DemoTile>
                ))}
              </GridItems>
            </Grid>
          </Fragment>
        ))}
      </Box>
      <DemoSetupDialog demo={setupDemo} onClose={() => setSetupDemo(undefined)}></DemoSetupDialog>
      <DemoCleanupDialog demo={cleanupDemo} onClose={() => setCleanupDemo(undefined)}></DemoCleanupDialog>
    </>
  )
}
