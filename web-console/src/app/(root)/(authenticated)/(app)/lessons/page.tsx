'use client'

import publicDemos from '$demo/publicDemos.json'
import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import { GridItems } from '$lib/components/common/GridItems'
import { DemoSetupDialog } from '$lib/components/demo/DemoSetupDialog'
import { Fragment, useState } from 'react'
import { DemoSetup } from 'src/lib/types/demo'
import IconChevronRight from '~icons/bx/chevron-right'

import { Box, Button, Card, CardActions, CardContent, Grid, Link, Typography } from '@mui/material'

const DemoTile = (props: { name: string; desc: string; onSetup: () => void }) => {
  return (
    <Card>
      <CardContent>
        <Typography variant='h5' gutterBottom>
          {props.name}
        </Typography>
        <Typography variant='body1'>{props.desc}</Typography>
      </CardContent>
      <CardActions sx={{ justifyContent: 'end' }}>
        <Button onClick={props.onSetup} endIcon={<IconChevronRight />}>
          Try
        </Button>
      </CardActions>
    </Card>
  )
}

export default function () {
  const [demo, setDemo] = useState<{ name: string; setup: DemoSetup } | undefined>()
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/lessons`} data-testid='button-breadcrumb-lessons'>
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
                    onSetup={() =>
                      import(`$demo/${demo.import}`)
                        .then(imported => imported.default)
                        .then(setup => setDemo({ name: demo.name, setup }))
                    }
                  ></DemoTile>
                ))}
              </GridItems>
            </Grid>
          </Fragment>
        ))}
      </Box>
      <DemoSetupDialog demo={demo} onClose={() => setDemo(undefined)}></DemoSetupDialog>
    </>
  )
}
