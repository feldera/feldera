import { describe, expect, it, vi } from 'vitest'
import type { PipelineThumb } from '$lib/services/pipelineManager'
import {
  duplicatePipeline,
  MAX_DUPLICATE_ATTEMPTS,
  resolveDuplicatePipelineName
} from './duplicatePipeline'

describe('resolveDuplicatePipelineName', () => {
  it('uses -copy for the first duplicate', () => {
    expect(resolveDuplicatePipelineName('pipeline', [{ name: 'pipeline' }])).toBe('pipeline-copy')
  })

  it('increments from 2 after the first copy exists', () => {
    expect(
      resolveDuplicatePipelineName('pipeline', [
        { name: 'pipeline' },
        { name: 'pipeline-copy' },
        { name: 'pipeline-copy2' }
      ])
    ).toBe('pipeline-copy3')
  })

  it('uses -copy when later copies exist but the first copy does not', () => {
    expect(
      resolveDuplicatePipelineName('orders', [
        { name: 'orders' },
        { name: 'orders-copy2' },
        { name: 'orders-copy3' }
      ])
    ).toBe('orders-copy')
  })

  it('continues after consecutive copy names', () => {
    expect(
      resolveDuplicatePipelineName('orders', [
        { name: 'orders' },
        { name: 'orders-copy' },
        { name: 'orders-copy2' },
        { name: 'orders-copy3' }
      ])
    ).toBe('orders-copy4')
  })

  it('continues the base sequence when duplicating the first copy', () => {
    expect(
      resolveDuplicatePipelineName('orders-copy', [{ name: 'orders' }, { name: 'orders-copy' }])
    ).toBe('orders-copy2')
  })

  it('continues the base sequence when duplicating a numbered copy', () => {
    expect(
      resolveDuplicatePipelineName('orders-copy2', [
        { name: 'orders' },
        { name: 'orders-copy' },
        { name: 'orders-copy2' }
      ])
    ).toBe('orders-copy3')
  })

  it('fills the first available gap', () => {
    expect(
      resolveDuplicatePipelineName('pipeline', [
        { name: 'pipeline' },
        { name: 'pipeline-copy' },
        { name: 'pipeline-copy3' }
      ])
    ).toBe('pipeline-copy2')
  })

  it('fails when no duplicate name can be allocated', () => {
    const pipelines = Array.from({ length: MAX_DUPLICATE_ATTEMPTS }, (_, index) => ({
      name: `pipeline-copy${index === 0 ? '' : index + 1}`
    }))

    expect(() => resolveDuplicatePipelineName('pipeline', pipelines)).toThrow(
      'Could not allocate a unique duplicate pipeline name.'
    )
  })
})

describe('duplicatePipeline', () => {
  it('copies persisted pipeline fields into the create request and updates the cache', async () => {
    const sourcePipeline = { name: 'pipeline' } as PipelineThumb
    const createdPipeline = { name: 'pipeline-copy' } as PipelineThumb
    let cachedPipelines = [sourcePipeline]
    const updates = {
      discardPendingListRefresh: vi.fn(),
      updatePipeline: vi.fn(
        (pipelineName: string, updater: (pipeline: PipelineThumb) => PipelineThumb) => {
          cachedPipelines = cachedPipelines.map((pipeline) =>
            pipeline.name === pipelineName ? updater(pipeline) : pipeline
          )
        }
      ),
      updatePipelines: vi.fn((updater: (pipelines: PipelineThumb[]) => PipelineThumb[]) => {
        cachedPipelines = updater(cachedPipelines)
      })
    }
    const api = {
      getExtendedPipeline: vi.fn().mockResolvedValue({
        description: 'source description',
        tags: ['demo'],
        runtimeConfig: { workers: 2 },
        programConfig: { profile: 'dev' },
        programCode: 'create table t (id int);',
        programUdfRs: 'pub fn udf() {}',
        programUdfToml: '[package]'
      }),
      postPipeline: vi.fn().mockResolvedValue(createdPipeline)
    }

    await expect(
      duplicatePipeline(api as never, sourcePipeline, cachedPipelines, updates)
    ).resolves.toBe(createdPipeline)

    expect(api.getExtendedPipeline).toHaveBeenCalledWith('pipeline')
    expect(api.postPipeline).toHaveBeenCalledWith({
      name: 'pipeline-copy',
      description: 'source description',
      tags: ['demo'],
      runtime_config: { workers: 2 },
      program_config: { profile: 'dev' },
      program_code: 'create table t (id int);',
      udf_rust: 'pub fn udf() {}',
      udf_toml: '[package]'
    })
    expect(updates.discardPendingListRefresh).toHaveBeenCalledOnce()
    expect(updates.updatePipelines).toHaveBeenCalledOnce()
    expect(updates.updatePipeline).toHaveBeenCalledWith('pipeline-copy', expect.any(Function))
    expect(cachedPipelines).toEqual([sourcePipeline, createdPipeline])
  })
})
