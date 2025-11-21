// Browser-based bundle processor using but-unzip
// Extracts profile and dataflow files from support bundles

import { unzip } from 'but-unzip';
import type { JsonProfiles, Dataflow } from 'profiler-lib';

export interface BundleFiles {
    profile: JsonProfiles;
    dataflow: Dataflow;
    programCode: string[] | undefined;
}

/**
 * Process a support bundle file in the browser
 * Extracts:
 * - *_circuit_profile.json - Profile data
 * - *_dataflow_graph.json - Dataflow graph
 */
export async function processBundleInBrowser(file: File): Promise<BundleFiles> {
    // Read the file as ArrayBuffer
    const arrayBuffer = await file.arrayBuffer();

    // Unzip the bundle
    const files = await unzip(new Uint8Array(arrayBuffer));
    console.log('files', files)

    // Find the profile file (*_circuit_profile.json)
    const profileEntry = Object.entries(files).find((file) =>
        file[1].filename.endsWith('_circuit_profile.json')
    );

    if (!profileEntry) {
        throw new Error('No profile file (*_circuit_profile.json) found in bundle');
    }

    // Find the dataflow file (*_dataflow_graph.json)
    const dataflowEntry = Object.entries(files).find((file) =>
        file[1].filename.endsWith('_dataflow_graph.json')
    );

    if (!dataflowEntry) {
        throw new Error('No dataflow file (*_dataflow_graph.json) found in bundle');
    }

    // Find the file with program code (*_pipeline_config.json)
    const pipelineConfigEntry = Object.entries(files).find((file) =>
        file[1].filename.endsWith('_pipeline_config.json')
    );

    // Parse the JSON files
    const profileText = new TextDecoder().decode(await profileEntry[1].read());
    const dataflowText = new TextDecoder().decode(await dataflowEntry[1].read());
    const pipelineConfigText = pipelineConfigEntry ? new TextDecoder().decode(await pipelineConfigEntry[1].read()) : undefined;

    return {
        profile: JSON.parse(profileText) as JsonProfiles,
        dataflow: JSON.parse(dataflowText) as Dataflow,
        programCode: pipelineConfigText ? (JSON.parse(pipelineConfigText).program_code as string).split('\n') : undefined
    };
}
