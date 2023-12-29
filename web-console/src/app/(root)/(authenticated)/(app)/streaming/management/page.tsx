'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import PipelineTable from '$lib/components/streaming/management/PipelineTable'
import { ErrorBoundary } from 'react-error-boundary'

import { Box, Link } from '@mui/material'

const PipelineManagement = () => {
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/streaming/management`} data-testid='button-breadcrumb-pipelines'>
          Pipelines
        </Link>
      </BreadcrumbsHeader>
      <Box>
        <ErrorBoundary FallbackComponent={ErrorOverlay}>
          <PipelineTable />
        </ErrorBoundary>
      </Box>
    </>
  )
}

export default PipelineManagement
