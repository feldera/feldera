import { $ } from 'bun'
import semver from 'semver'

const MIN_BUN_VERSION = '1.3.3'

async function checkBunVersion() {
  try {
    // Get current Bun version
    const result = await $`bun --version`.text()
    const currentVersion = result.trim()

    console.log(`Current Bun version: ${currentVersion}`)
    console.log(`Required minimum version: ${MIN_BUN_VERSION}`)

    // Compare versions
    if (!semver.gte(currentVersion, MIN_BUN_VERSION)) {
      console.error(
        `\n❌ Error: Bun version ${currentVersion} is too old. Please upgrade to ${MIN_BUN_VERSION} or higher.`
      )
      console.error(`   Run: bun upgrade`)
      process.exit(1)
    }

    console.log(`✓ Bun version check passed\n`)
  } catch (error) {
    console.error('Failed to check Bun version:', error)
    process.exit(1)
  }
}

checkBunVersion()
