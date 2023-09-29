'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import PipelineTable from '$lib/components/streaming/management/PipelineTable'
import { ErrorBoundary } from 'react-error-boundary'

import { Link } from '@mui/material'
import Grid from '@mui/material/Grid'

const PipelineManagement = () => {
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/streaming/management`}>Pipelines</Link>
      </BreadcrumbsHeader>
      {/* id referenced by webui-tester */}
      <Grid item xs={12} id='pipeline-management-content'>
        <ErrorBoundary FallbackComponent={ErrorOverlay}>
          <PipelineTable />
        </ErrorBoundary>
      </Grid>
    </>
  )
}

export default PipelineManagement
