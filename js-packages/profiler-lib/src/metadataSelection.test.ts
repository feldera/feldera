/**
 * The metric selector lists the metrics a profile carries. Issue 6991: it listed the raw ids
 * (`spine_storage_size_bytes`) while the table of values spelled them out ("Spine storage size
 * bytes"), so the same metric appeared under two names. Both now go through `measurementLabel`.
 */

import { describe, expect, it } from 'vitest'
import { MetadataSelector } from './metadataSelection.js'
import { CircuitProfile, compareMetrics, type JsonProfiles } from './profile.js'
import type { MetricOption, ProfilerCallbacks } from './profiler.js'

// One operator carrying a byte metric, a percentage, and a composite batch-size summary.
const profile = (() => {
    const metadata = {
        nn1: [
            { metric_id: 'spine_storage_size_bytes', value: { type: 'bytes', value: 2048 } },
            { metric_id: 'runtime_percent', value: { type: 'percent', value: { numerator: 1, denominator: 4 } } },
            {
                metric_id: 'input_batches_stats',
                value: {
                    batches_count: { type: 'count', value: 2 },
                    min_records_count: { type: 'count', value: 1 },
                    max_records_count: { type: 'count', value: 9 },
                    avg_records_count: { type: 'count', value: 5 },
                    total_records_count: { type: 'count', value: 10 }
                }
            }
        ]
    }
    const json = {
        metrics: [],
        worker_profiles: [{ metadata }],
        graph: {
            nodes: { id: 'n', label: 'root', nodes: [{ Simple: { id: 'nn1', label: 'op' } }] },
            edges: []
        }
    }
    return CircuitProfile.fromJson(json as unknown as JsonProfiles).profile
})()

const options = (): MetricOption[] => {
    let listed: MetricOption[] = []
    const callbacks = {
        onMetricsChanged: (metrics: MetricOption[]) => {
            listed = metrics
        }
    } as unknown as ProfilerCallbacks
    new MetadataSelector(profile, callbacks).initialize()
    return listed
}

describe('MetadataSelector metric options', () => {
    it('labels each metric the way the tables spell it', () => {
        const byId = new Map(options().map((o) => [o.id, o.label]))
        expect(byId.get('spine_storage_size_bytes')).toBe('Spine storage size bytes')
        expect(byId.get('runtime_percent')).toBe('Runtime percent')
        expect(byId.get('input_batches_stats.min_size')).toBe('Input batches stats min size')
    })

    it('keeps the id as the value the selector reports back', () => {
        // The label is for reading; selection still travels by id.
        for (const option of options()) {
            expect(profile.simpleNodes.get('nn1').unwrap().getMeasurements(option.id).length)
                .toBeGreaterThan(0)
        }
    })

    it('lists the metrics in the order the tables use', () => {
        const listed = options().map((o) => ({ id: o.id, label: o.label }))
        expect(listed).toEqual([...listed].sort(compareMetrics))
    })
})
