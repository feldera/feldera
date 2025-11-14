#!/usr/bin/env node
// Server entry point for the profiler app
// This script processes support bundles at runtime, then starts the appropriate server
//
// This script:
// - Extracts profile and config files from a support bundle
// - Generates dataflow graph from SQL using the compiler
// - Saves files to data/ directory
// - Starts the web server (Vite dev or static file server)
//
// Usage:
//   Development mode:
//     bun run dev                                    # Start without bundle processing
//     BUNDLE=/path/to/bundle.zip bun run dev         # Process bundle then start dev server
//     BUNDLE=/path/to/bundle.zip VERBOSE=1 bun run dev  # With verbose output
//
//   Production mode (after running 'bun run build'):
//     bun run start                                  # Serve built files
//     BUNDLE=/path/to/bundle.zip bun run start       # Process bundle then serve
//     BUNDLE=/path/to/bundle.zip BUNDLE_NAME=my-profile bun run start  # Custom name
//
//   Direct execution:
//     node launcher.ts                                    # Auto-detects dev/production
//     BUNDLE=/path/to/bundle.zip node launcher.ts         # With bundle processing

import { BundleProcessor, getBundleFromEnv } from './src/bundleProcessor.js';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { existsSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function main() {
    // Process bundle if BUNDLE env var is set
    const bundleOptions = getBundleFromEnv();

    if (bundleOptions) {
        try {
            console.log('[Profiler] Processing support bundle...');
            const processor = new BundleProcessor(bundleOptions);
            const outputName = await processor.process();
            console.log(`[Profiler] Bundle extracted successfully: ${outputName}`);

            // Write config file so browser knows which profile to load
            const { writeFileSync } = await import('fs');
            const configPath = join(__dirname, 'data', 'profile-config.json');
            writeFileSync(configPath, JSON.stringify({ profileName: outputName }));
        } catch (error) {
            console.error('[Profiler] Failed to extract bundle:', error);
            process.exit(1);
        }
    }

    // Determine if we're in dev mode or production
    const distPath = join(__dirname, 'dist');
    const isProduction = existsSync(distPath);

    if (isProduction) {
        // Production: serve static files from dist/
        console.log('[Profiler] Starting production server...');
        const serve = spawn('npx', ['serve', 'dist', '-p', '5174'], {
            stdio: 'inherit',
            shell: true,
            cwd: __dirname
        });

        serve.on('exit', (code) => {
            process.exit(code ?? 0);
        });
    } else {
        // Development: start Vite dev server
        console.log('[Profiler] Starting development server...');
        const vite = spawn('npx', ['vite', '--clearScreen', 'false', '--host'], {
            stdio: 'inherit',
            shell: true,
            cwd: __dirname
        });

        vite.on('exit', (code) => {
            process.exit(code ?? 0);
        });
    }
}

main().catch((error) => {
    console.error('[Profiler] Fatal error:', error);
    process.exit(1);
});
