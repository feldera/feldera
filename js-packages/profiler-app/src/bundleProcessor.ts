// Support bundle processing utilities for the standalone profiler app

import * as fs from 'fs/promises';
import * as path from 'path';
import { execSync } from 'child_process';
import * as zlib from 'zlib';
import { promisify } from 'util';
import AdmZip from 'adm-zip';

const gunzip = promisify(zlib.gunzip);

export interface BundleProcessOptions {
    bundlePath: string;
    verbose?: boolean;
    outputName?: string;
}

/**
 * Process a support bundle file:
 * - Extract profile and config files
 * - Generate dataflow graph from SQL
 * - Save files to data/ directory
 */
export class BundleProcessor {
    private verbose: boolean;
    private outputName: string;

    constructor(private options: BundleProcessOptions) {
        this.verbose = options.verbose ?? false;
        this.outputName = options.outputName ?? 'temp';
    }

    private log(message: string): void {
        if (this.verbose) {
            console.log(message);
        }
    }

    private async extractProfileFile(zip: AdmZip): Promise<void> {
        this.log('Extracting profile file...');
        const entries = zip.getEntries();

        // Find file matching pattern "*_circuit_profile.json"
        const profileFiles = entries.filter(e =>
            e.entryName.endsWith('_circuit_profile.json')
        );

        if (profileFiles.length === 0) {
            throw new Error('No json profile file found in the bundle');
        }

        // Choose the last one in sorted order
        const profileFile = profileFiles.sort((a, b) =>
            a.entryName.localeCompare(b.entryName)
        )[profileFiles.length - 1]!;

        this.log(`Found profile file: ${profileFile.entryName}`);

        // Extract the JSON file directly (no nested zip anymore)
        const jsonData = profileFile.getData();

        // Write to data directory
        const outputPath = path.join('data', `${this.outputName}.json`);
        await fs.writeFile(outputPath, jsonData);

        this.log(`Extracted profile file to: ${outputPath}`);
    }

    private async extractAndCompileSQL(zip: AdmZip): Promise<void> {
        this.log('Extracting config file...');
        const entries = zip.getEntries();

        // Find files matching "pipeline_config.json.gz"
        const configFiles = entries.filter(e =>
            e.entryName.endsWith('pipeline_config.json.gz')
        );

        if (configFiles.length === 0) {
            this.log('No config file found, skipping SQL compilation');
            return;
        }

        // Choose the last one in sorted order
        const configFile = configFiles.sort((a, b) =>
            a.entryName.localeCompare(b.entryName)
        )[configFiles.length - 1]!;

        this.log(`Found config file: ${configFile.entryName}`);

        // Extract and decompress the config file
        const gzippedData = configFile.getData();
        const jsonData = await gunzip(gzippedData);
        const config = JSON.parse(jsonData.toString('utf-8'));

        const programCode = config.program_code;
        if (!programCode) {
            throw new Error('No program_code found in config file');
        }

        // Write SQL to temporary file
        const sqlPath = path.join('data', 'temp-program.sql');
        await fs.writeFile(sqlPath, programCode, 'utf-8');
        this.log(`Extracted program code to: ${sqlPath}`);

        // Invoke SQL compiler to generate dataflow graph
        this.log('Invoking SQL to DBSP compiler to generate dataflow graph');
        const compilerPath = path.join('..', '..', 'sql-to-dbsp-compiler', 'SQL-compiler', 'sql-to-dbsp');
        const dataflowPath = path.join('data', `dataflow-${this.outputName}.json`);

        try {
            execSync(
                `${compilerPath} ${sqlPath} -i --dataflow ${dataflowPath} -q --alltables --noRust --ignoreOrder`,
                {
                    cwd: process.cwd(),
                    stdio: this.verbose ? 'inherit' : 'pipe'
                }
            );
            this.log(`Generated dataflow graph to: ${dataflowPath}`);
        } catch (error) {
            throw new Error(`SQL compiler failed: ${error}`);
        } finally {
            // Clean up temporary SQL file
            await fs.unlink(sqlPath);
            this.log('Deleted temporary SQL file');
        }
    }

    async process(): Promise<string> {
        const bundlePath = path.resolve(this.options.bundlePath);

        // Verify bundle file exists
        try {
            await fs.access(bundlePath);
        } catch {
            throw new Error(`Bundle file not found: ${bundlePath}`);
        }

        this.log(`Processing bundle: ${bundlePath}`);

        // Ensure data directory exists
        await fs.mkdir('data', { recursive: true });

        // Open the zip file
        const zip = new AdmZip(bundlePath);

        // Extract profile data
        await this.extractProfileFile(zip);

        // Extract config and compile SQL
        await this.extractAndCompileSQL(zip);

        this.log('Bundle processing complete');
        return this.outputName;
    }
}

/**
 * Check for BUNDLE environment variable and return processing options if set
 *
 * Environment variables:
 *   BUNDLE=<path>       - Path to support bundle file
 *   VERBOSE=1           - Enable verbose output
 *   BUNDLE_NAME=<name>  - Output name (default: temp)
 */
export function getBundleFromEnv(): BundleProcessOptions | null {
    const bundlePath = process.env['BUNDLE'];

    if (!bundlePath) {
        return null;
    }

    const verbose = process.env['VERBOSE']! === '1' || process.env['VERBOSE']! === 'true';
    const outputName = process.env['BUNDLE_NAME']!;

    return { bundlePath, verbose, outputName };
}
