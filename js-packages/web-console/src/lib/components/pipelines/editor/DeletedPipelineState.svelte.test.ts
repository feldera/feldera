/**
 * Integration UI tests for the "deleted pipeline" state.
 *
 * Requires a running Feldera instance. Run with:
 *   bun run test-integration
 *
 * Creates a real pipeline via the API, renders each touched component with
 * deleted=true / deleted=false, and verifies the DOM reacts correctly.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import {
  deletePipeline,
  getExtendedPipeline,
  getPipelines,
  type PipelineThumb,
  putPipeline
} from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  type ExtendedPipeline
} from '$lib/services/testPipelineHelpers'

// Mock SvelteKit's $app/state for components that read `page.data.feldera`
// and `page.url` (the CodeEditor parses positions from the URL hash).
vi.mock('$app/state', () => ({
  page: {
    data: {
      feldera: {
        version: 'test-runtime',
        unstableFeatures: [],
        edition: 'Open Source'
      }
    },
    url: new URL('http://localhost/')
  }
}))

import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'
import { emptyPipelineMetrics, type PipelineMetrics } from '$lib/functions/pipelineMetrics'
import ActionsFixture from './ActionsFixture.svelte'
import InteractionPanel from './InteractionPanel.svelte'
import MonitoringPanel from './MonitoringPanel.svelte'
import TabAdHocQuery from './TabAdHocQuery.svelte'
import TabChangeStream from './TabChangeStream.svelte'
import TabLogs from './TabLogs.svelte'
import TabPerformance from './TabPerformance.svelte'
import TabProfileVisualizer from './TabProfileVisualizer.svelte'
import TabSamplyProfile from './TabSamplyProfile.svelte'

const PIPELINE_NAME = 'test-deleted-pipeline-state'

let pipeline: ExtendedPipeline
let pipelines: PipelineThumb[]

function pipelineProp(): { current: ExtendedPipeline } {
  return { current: pipeline }
}

function metricsProp(): { current: PipelineMetrics } {
  return { current: emptyPipelineMetrics }
}

function writablePipelineProp(): WritablePipeline {
  return {
    get current() {
      return pipeline
    },
    async patch() {
      return pipeline
    }
  }
}

describe('Deleted pipeline state', () => {
  beforeAll(async () => {
    configureTestClient()
    await cleanupPipeline(PIPELINE_NAME)
    await putPipeline(PIPELINE_NAME, {
      name: PIPELINE_NAME,
      program_code: 'CREATE TABLE t1 (id INT);',
      runtime_config: {}
    })
    pipeline = await getExtendedPipeline(PIPELINE_NAME)
    pipelines = await getPipelines()
  }, 60_000)

  afterAll(async () => {
    await cleanupPipeline(PIPELINE_NAME)
  }, 30_000)

  describe('PipelineEditLayout', () => {
    it('shows deleted chip and banner when deleted', async () => {
      const { unmount } = render(PipelineEditLayout, {
        pipeline: writablePipelineProp(),
        preloaded: { pipelines },
        deleted: true
      })
      const banner = page.getByText('This pipeline has been deleted')
      await expect.element(banner).toBeInTheDocument()
      unmount()
    })

    it('does not show deleted banner when not deleted', async () => {
      const { unmount } = render(PipelineEditLayout, {
        pipeline: writablePipelineProp(),
        preloaded: { pipelines },
        deleted: false
      })
      const banner = page.getByText('This pipeline has been deleted')
      await expect.element(banner).not.toBeInTheDocument()
      unmount()
    })
  })

  describe('Actions', () => {
    it('shows action buttons when not deleted', async () => {
      const { unmount } = render(ActionsFixture, {
        pipeline: pipelineProp(),
        deleted: false
      })
      await expect.element(page.getByTestId('box-action-buttons')).toBeInTheDocument()
      unmount()
    })

    it('hides action buttons when deleted', async () => {
      const { unmount } = render(ActionsFixture, {
        pipeline: pipelineProp(),
        deleted: true
      })
      await expect.element(page.getByTestId('box-action-buttons')).not.toBeInTheDocument()
      unmount()
    })
  })

  describe('TabLogs', () => {
    it('shows deleted banner when deleted', async () => {
      const { unmount } = render(TabLogs, {
        pipeline: pipelineProp(),
        deleted: true
      })
      const banner = page.getByText('The pipeline has been deleted.')
      await expect.element(banner).toBeInTheDocument()
      unmount()
    })

    it('does not show deleted banner when not deleted', async () => {
      const { unmount } = render(TabLogs, {
        pipeline: pipelineProp(),
        deleted: false
      })
      const banner = page.getByText('The pipeline has been deleted.')
      await expect.element(banner).not.toBeInTheDocument()
      unmount()
    })
  })

  describe('TabPerformance', () => {
    it('renders without error when deleted', async () => {
      const { unmount } = render(TabPerformance, {
        pipeline: pipelineProp(),
        metrics: metricsProp(),
        deleted: true
      })
      const msg = page.getByText('Pipeline is not running')
      await expect.element(msg).toBeInTheDocument()
      unmount()
    })

    it('renders without error when not deleted', async () => {
      const { unmount } = render(TabPerformance, {
        pipeline: pipelineProp(),
        metrics: metricsProp(),
        deleted: false
      })
      const msg = page.getByText('Pipeline is not running')
      await expect.element(msg).toBeInTheDocument()
      unmount()
    })
  })

  describe('TabAdHocQuery', () => {
    it('shows deleted banner when deleted', async () => {
      const { unmount } = render(TabAdHocQuery, {
        pipeline: pipelineProp(),
        deleted: true
      })
      await expect
        .element(page.getByTestId('box-adhoc-banner'))
        .toHaveTextContent('The pipeline has been deleted')
      unmount()
    })

    it('shows start-pipeline banner when not deleted and stopped', async () => {
      const { unmount } = render(TabAdHocQuery, {
        pipeline: pipelineProp(),
        deleted: false
      })
      await expect
        .element(page.getByTestId('box-adhoc-banner'))
        .toHaveTextContent('Start the pipeline to be able to run queries')
      unmount()
    })
  })

  describe('TabChangeStream', () => {
    it('shows deleted banner when deleted', async () => {
      const { unmount } = render(TabChangeStream, {
        pipeline: pipelineProp(),
        deleted: true
      })
      await expect
        .element(page.getByTestId('box-changestream-banner'))
        .toHaveTextContent('The pipeline has been deleted')
      unmount()
    })

    it('does not show deleted banner when not deleted', async () => {
      const { unmount } = render(TabChangeStream, {
        pipeline: pipelineProp(),
        deleted: false
      })
      await expect.element(page.getByTestId('box-changestream-banner')).not.toBeInTheDocument()
      unmount()
    })
  })

  describe('TabSamplyProfile', () => {
    it('disables collect button when deleted', async () => {
      const { unmount } = render(TabSamplyProfile, {
        pipeline: pipelineProp(),
        deleted: true
      })
      const btn = page.getByText('Collect profile')
      await expect.element(btn).toBeDisabled()
      unmount()
    })

    it('shows start-pipeline warning when not deleted and stopped', async () => {
      const { unmount } = render(TabSamplyProfile, {
        pipeline: pipelineProp(),
        deleted: false
      })
      const warning = page.getByText('Start the pipeline to collect the profile')
      await expect.element(warning).toBeInTheDocument()
      unmount()
    })
  })

  describe('TabProfileVisualizer', () => {
    it('disables download button when deleted', async () => {
      const { unmount } = render(TabProfileVisualizer, {
        pipeline: pipelineProp(),
        deleted: true
      })
      const btn = page.getByText('Download pipeline profile')
      await expect.element(btn).toBeDisabled()
      unmount()
    })

    it('enables download button when not deleted', async () => {
      const { unmount } = render(TabProfileVisualizer, {
        pipeline: pipelineProp(),
        deleted: false
      })
      const btn = page.getByText('Download pipeline profile')
      await expect.element(btn).not.toBeDisabled()
      unmount()
    })
  })

  describe('MonitoringPanel', () => {
    it('renders tabs when not deleted', async () => {
      const { unmount } = render(MonitoringPanel, {
        pipeline: pipelineProp(),
        metrics: metricsProp(),
        deleted: false,
        hiddenTabs: [],
        currentTab: 'Errors'
      })
      const tab = page.getByText('Compiler')
      await expect.element(tab).toBeInTheDocument()
      unmount()
    })

    it('renders tabs when deleted', async () => {
      const { unmount } = render(MonitoringPanel, {
        pipeline: pipelineProp(),
        metrics: metricsProp(),
        deleted: true,
        hiddenTabs: [],
        currentTab: 'Errors'
      })
      const tab = page.getByText('Compiler')
      await expect.element(tab).toBeInTheDocument()
      unmount()
    })
  })

  describe('InteractionPanel', () => {
    it('renders tabs when not deleted', async () => {
      const { unmount } = render(InteractionPanel, {
        pipeline: pipelineProp(),
        metrics: metricsProp(),
        deleted: false,
        currentTab: null
      })
      const tab = page.getByText('Ad-Hoc Queries')
      await expect.element(tab).toBeInTheDocument()
      unmount()
    })

    it('renders tabs when deleted', async () => {
      const { unmount } = render(InteractionPanel, {
        pipeline: pipelineProp(),
        metrics: metricsProp(),
        deleted: true,
        currentTab: null
      })
      const tab = page.getByText('Ad-Hoc Queries')
      await expect.element(tab).toBeInTheDocument()
      unmount()
    })
  })

  describe('API deletion detection', () => {
    it('getExtendedPipeline calls onNotFound after pipeline is deleted', async () => {
      const tempName = 'test-deleted-detection'
      await cleanupPipeline(tempName)
      await putPipeline(tempName, {
        name: tempName,
        program_code: 'CREATE TABLE t2 (id INT);',
        runtime_config: {}
      })

      const p = await getExtendedPipeline(tempName)
      expect(p.name).toBe(tempName)

      await deletePipeline(tempName)

      let notFoundCalled = false
      try {
        await getExtendedPipeline(tempName, {
          onNotFound: () => {
            notFoundCalled = true
          }
        })
      } catch {
        // Expected to throw after calling onNotFound
      }
      expect(notFoundCalled).toBe(true)

      const list = await getPipelines()
      expect(list.find((p) => p.name === tempName)).toBeUndefined()
    })
  })
})
