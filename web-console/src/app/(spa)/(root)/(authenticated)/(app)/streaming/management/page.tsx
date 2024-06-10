'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import PipelineTable from '$lib/components/streaming/management/PipelineTable'
import { ErrorBoundary } from 'react-error-boundary'

import { Box } from '@mui/material'

const PipelineManagement = () => {
  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/streaming/management`} data-testid='button-breadcrumb-pipelines'>
          Pipelines
        </Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <Box>
        <ErrorBoundary FallbackComponent={ErrorOverlay}>
          <PipelineTable />
        </ErrorBoundary>
      </Box>
    </>
  )
}

export default PipelineManagement
