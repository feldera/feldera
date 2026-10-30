// Downloads and publishes the reference screenshots of the tests.
//
// The screenshots live in https://github.com/feldera/playwright-snapshots, not in this repository.
// `playwright-snapshots.commit` pins the commit of that repository that the tests use.
//
//   bun scripts/playwright-snapshots.ts pull   Downloads the pinned commit to `playwright-snapshots/`.
//   bun scripts/playwright-snapshots.ts push   Commits `playwright-snapshots/` as the full contents
//                                              of the repository, and pins the new commit.

import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync
} from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { $ } from 'bun'

const REPO = 'feldera/playwright-snapshots'

const packageDir = path.resolve(import.meta.dir, '..')
const snapshotsDir = path.join(packageDir, 'playwright-snapshots')
const pinFile = path.join(packageDir, 'playwright-snapshots.commit')
/** The commit that `snapshotsDir` holds, and the hashes of its files. It is not published. */
const manifestFile = path.join(snapshotsDir, '.manifest.json')
/** The entries of `snapshotsDir` that are not screenshots. */
const LOCAL_ENTRIES = ['.manifest.json', '.git']

type Manifest = { commit: string; hashes: Record<string, string> }

/** Hashes each screenshot file, by its path relative to `snapshotsDir`. */
const hashFiles = (): Record<string, string> =>
  Object.fromEntries(
    readdirSync(snapshotsDir, { recursive: true, withFileTypes: true })
      .filter((entry) => entry.isFile() && !LOCAL_ENTRIES.includes(entry.name))
      .map((entry) => {
        const file = path.join(entry.parentPath, entry.name)
        return [path.relative(snapshotsDir, file), Bun.hash(readFileSync(file)).toString(16)]
      })
  )

const writeManifest = (commit: string) =>
  writeFileSync(manifestFile, JSON.stringify({ commit, hashes: hashFiles() } satisfies Manifest))

const readManifest = (): Manifest | undefined =>
  existsSync(manifestFile) ? JSON.parse(readFileSync(manifestFile, 'utf8')) : undefined

/** Lists the screenshots that were added or changed after the download of `manifest.commit`. */
const listChangedFiles = (manifest: Manifest) =>
  Object.entries(hashFiles())
    .filter(([file, hash]) => manifest.hashes[file] !== hash)
    .map(([file]) => file)

async function pull() {
  const commit = readFileSync(pinFile, 'utf8').trim()
  const manifest = readManifest()
  if (manifest?.commit === commit) {
    return
  }
  // A test run with `--update` writes new screenshots. Do not delete them before they are pushed.
  if (manifest) {
    const changed = listChangedFiles(manifest)
    if (changed.length > 0) {
      throw new Error(
        `The pinned commit changed, but these screenshots are not pushed:\n  ${changed.join('\n  ')}\n` +
          'Run `bun run test-snapshots-push`, or delete playwright-snapshots/ to discard them.'
      )
    }
  }
  const response = await fetch(`https://codeload.github.com/${REPO}/tar.gz/${commit}`)
  if (!response.ok) {
    throw new Error(`Cannot download ${REPO}@${commit}: HTTP ${response.status}`)
  }
  const archive = path.join(mkdtempSync(path.join(tmpdir(), 'playwright-snapshots-')), 'a.tar.gz')
  await Bun.write(archive, response)
  rmSync(snapshotsDir, { recursive: true, force: true })
  mkdirSync(snapshotsDir)
  await $`tar -xzf ${archive} --strip-components=1 -C ${snapshotsDir}`
  writeManifest(commit)
  console.log(`Downloaded the screenshots of ${REPO}@${commit.slice(0, 12)}`)
}

async function push() {
  const workDir = mkdtempSync(path.join(tmpdir(), 'playwright-snapshots-'))
  await $`git clone -q --depth 1 git@github.com:${REPO}.git ${workDir}`
  await $`git -C ${workDir} rm -r -q --ignore-unmatch .`
  for (const entry of readdirSync(snapshotsDir)) {
    if (!LOCAL_ENTRIES.includes(entry)) {
      cpSync(path.join(snapshotsDir, entry), path.join(workDir, entry), { recursive: true })
    }
  }
  await $`git -C ${workDir} add -A`
  const hasChanges = (await $`git -C ${workDir} diff --cached --quiet`.nothrow()).exitCode !== 0
  if (hasChanges) {
    const source = (await $`git -C ${packageDir} rev-parse --short HEAD`.text()).trim()
    await $`git -C ${workDir} commit -q -m ${`Update web-console screenshots (feldera ${source})`}`
    await $`git -C ${workDir} push -q origin HEAD:main`
  }
  const commit = (await $`git -C ${workDir} rev-parse HEAD`.text()).trim()
  writeFileSync(pinFile, `${commit}\n`)
  writeManifest(commit)
  console.log(
    hasChanges
      ? `Pushed ${REPO}@${commit.slice(0, 12)}. Commit playwright-snapshots.commit.`
      : `No changes. Pinned ${REPO}@${commit.slice(0, 12)}.`
  )
}

const commands: Record<string, () => Promise<void>> = { pull, push }
const command = commands[process.argv[2]]
if (!command) {
  console.error('Usage: bun scripts/playwright-snapshots.ts pull|push')
  process.exit(1)
}
await command()
