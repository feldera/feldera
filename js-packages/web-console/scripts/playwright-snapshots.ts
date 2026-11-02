// Downloads and publishes the reference screenshots of the tests.
//
// The screenshots live in https://github.com/feldera/playwright-snapshots, not in this repository.
// `playwright-snapshots.commit` pins the commit of that repository that the tests use.
//
//   bun scripts/playwright-snapshots.ts pull   Downloads the pinned commit to `playwright-snapshots/`.
//   bun scripts/playwright-snapshots.ts push   Commits the screenshots that changed after the pull
//                                              on top of the latest commit of that repository, and
//                                              pins the new commit.
//
// `push` sends only the changed screenshots. Thus, two branches that update different screenshots
// at the same time do not delete the screenshots of each other.

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

/** Lists the screenshots that were deleted after the download of `manifest.commit`. */
const listDeletedFiles = (manifest: Manifest) => {
  const hashes = hashFiles()
  return Object.keys(manifest.hashes).filter((file) => !(file in hashes))
}

const DOWNLOAD_ATTEMPTS = 3

/** Downloads the archive of `commit`. Retries a failed download, because the network can fail. */
async function download(commit: string): Promise<Response> {
  const url = `https://codeload.github.com/${REPO}/tar.gz/${commit}`
  let error: unknown
  for (let attempt = 1; attempt <= DOWNLOAD_ATTEMPTS; attempt++) {
    try {
      const response = await fetch(url)
      if (response.ok) {
        return response
      }
      error = new Error(`HTTP ${response.status}`)
    } catch (e) {
      error = e
    }
    console.warn(`Cannot download ${REPO}@${commit} (attempt ${attempt}): ${error}`)
  }
  throw new Error(`Cannot download ${REPO}@${commit}: ${error}`)
}

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
  const response = await download(commit)
  const archive = path.join(mkdtempSync(path.join(tmpdir(), 'playwright-snapshots-')), 'a.tar.gz')
  await Bun.write(archive, response)
  rmSync(snapshotsDir, { recursive: true, force: true })
  mkdirSync(snapshotsDir)
  await $`tar -xzf ${archive} --strip-components=1 -C ${snapshotsDir}`
  writeManifest(commit)
  console.log(`Downloaded the screenshots of ${REPO}@${commit.slice(0, 12)}`)
}

async function push() {
  const manifest = readManifest()
  if (!manifest) {
    throw new Error('Run `bun run test-prepare` first. It records the screenshots to compare with.')
  }
  const changed = listChangedFiles(manifest)
  const deleted = listDeletedFiles(manifest)
  // If a different branch pushes between the clone and the push, the push fails. Then the
  // loop applies the changes again to a new clone, which contains the screenshots of that branch.
  for (let attempt = 1; ; attempt++) {
    const workDir = mkdtempSync(path.join(tmpdir(), 'playwright-snapshots-'))
    await $`git clone -q --depth 1 git@github.com:${REPO}.git ${workDir}`
    const hasChanges = await commitChanges(workDir, changed, deleted)
    const isPushed =
      !hasChanges || (await $`git -C ${workDir} push -q origin HEAD:main`.nothrow()).exitCode === 0
    if (isPushed) {
      await pinClone(workDir, hasChanges)
      return
    }
    if (attempt === PUSH_ATTEMPTS) {
      throw new Error(`Cannot push to ${REPO}`)
    }
  }
}

const PUSH_ATTEMPTS = 3

/** Copies the changed screenshots to the clone in `workDir`, and commits them. Returns `false` if nothing changed. */
async function commitChanges(workDir: string, changed: string[], deleted: string[]) {
  for (const file of changed) {
    const target = path.join(workDir, file)
    mkdirSync(path.dirname(target), { recursive: true })
    cpSync(path.join(snapshotsDir, file), target)
  }
  for (const file of deleted) {
    rmSync(path.join(workDir, file), { force: true })
  }
  await $`git -C ${workDir} add -A`
  const hasChanges = (await $`git -C ${workDir} diff --cached --quiet`.nothrow()).exitCode !== 0
  if (hasChanges) {
    const source = (await $`git -C ${packageDir} rev-parse --short HEAD`.text()).trim()
    await $`git -C ${workDir} commit -q -m ${`Update web-console screenshots (feldera ${source})`}`
  }
  return hasChanges
}

/** Pins the commit of the clone in `workDir`, and replaces the local screenshots with its files. */
async function pinClone(workDir: string, hasChanges: boolean) {
  const commit = (await $`git -C ${workDir} rev-parse HEAD`.text()).trim()
  writeFileSync(pinFile, `${commit}\n`)
  // The commit can contain the screenshots that other branches pushed.
  rmSync(snapshotsDir, { recursive: true, force: true })
  cpSync(workDir, snapshotsDir, {
    recursive: true,
    filter: (source) => path.basename(source) !== '.git'
  })
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
