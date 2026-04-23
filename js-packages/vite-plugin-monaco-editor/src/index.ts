import { createRequire } from 'node:module'
import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import metadata, {
  type EditorFeature,
  type EditorLanguage,
  type IFeatureDefinition,
  type NegatedEditorFeature
} from 'monaco-editor/esm/metadata.js'
import type { Plugin, UserConfig } from 'vite'

export type Languages = '*' | 'all' | EditorLanguage[]

export type Features =
  | '*'
  | 'all'
  | Array<'codicons' | 'codicon' | EditorFeature | NegatedEditorFeature>

export interface MonacoOptions {
  features?: Features
  languages?: Languages
  customLanguages?: IFeatureDefinition[]
  globalAPI?: boolean
}

interface ResolvedWorker {
  label: string
  id: string
  entry: string
}

const EDITOR_MAIN_RE = /monaco-editor[/\\]esm[/\\]vs[/\\]editor[/\\]editor\.main\.js$/
const EDITOR_ALL_RE = /monaco-editor[/\\]esm[/\\]vs[/\\]editor[/\\]editor\.all\.js$/
const EDCORE_MAIN_RE = /monaco-editor[/\\]esm[/\\]vs[/\\]editor[/\\]edcore\.main\.js$/
// Files inside monaco-editor that contain bare side-effect imports of every
// editor contribution, defeating the selection emitted by the editor.main.js
// replacement. Rather than enumerating each (the list grows per monaco
// version), strip contrib/standalone side-effect imports from any monaco-editor
// source file that has them.
const MONACO_SRC_RE = /monaco-editor[/\\]esm[/\\]vs[/\\].+\.js$/

const require_ = createRequire(import.meta.url)

function resolveMonacoPath(relative: string): string {
  const specifier = `monaco-editor/esm/${relative.replace(/\.js$/, '')}.js`
  try {
    const resolved = require_.resolve(specifier)
    return stripFileScheme(resolved)
  } catch {
    // fallback: cwd/node_modules/monaco-editor/esm/<relative>
    const fallback = path.resolve(
      process.cwd(),
      'node_modules',
      'monaco-editor',
      'esm',
      relative.endsWith('.js') ? relative : `${relative}.js`
    )
    return fallback
  }
}

function stripFileScheme(p: string): string {
  if (p.startsWith('file://')) {
    return fileURLToPath(p)
  }
  return p
}

function resolveLanguages(
  option: Languages | undefined,
  customLanguages: IFeatureDefinition[]
): IFeatureDefinition[] {
  const all = metadata.languages as IFeatureDefinition[]
  let builtins: IFeatureDefinition[]
  if (option === undefined) {
    builtins = []
  } else if (option === '*' || option === 'all') {
    builtins = [...all]
  } else {
    const byLabel = new Map(all.map((d) => [d.label, d]))
    builtins = []
    for (const name of option) {
      const found = byLabel.get(name)
      if (found) {
        builtins.push(found)
      } else {
        console.error(
          `[@feldera/vite-plugin-monaco-editor] Unknown monaco language: "${name}" — dropped.`
        )
      }
    }
  }
  return [...builtins, ...customLanguages]
}

function resolveFeatures(
  option: Features | undefined,
  allFeatures: IFeatureDefinition[]
): IFeatureDefinition[] {
  if (option === undefined || option === '*' || option === 'all') {
    return [...allFeatures]
  }
  const byLabel = new Map(allFeatures.map((d) => [d.label, d]))
  const hasCodiconsAlias = byLabel.has('codicons') || byLabel.has('codicon')

  const plain: string[] = []
  const denied: string[] = []
  for (const entry of option) {
    if (entry.startsWith('!')) {
      denied.push(entry.slice(1))
    } else {
      plain.push(entry)
    }
  }

  const resolveName = (name: string): IFeatureDefinition | undefined => {
    const direct = byLabel.get(name)
    if (direct) return direct
    if (name === 'codicons' && byLabel.has('codicon')) return byLabel.get('codicon')
    if (name === 'codicon' && byLabel.has('codicons')) return byLabel.get('codicons')
    return undefined
  }

  // Deny-list only: everything except denied entries.
  if (denied.length > 0 && plain.length === 0) {
    const denySet = new Set(denied)
    return allFeatures.filter((d) => !denySet.has(d.label))
  }

  // Allow-list (or mixed — deterministic behavior: allow-list wins, deny entries ignored).
  const out: IFeatureDefinition[] = []
  const seen = new Set<string>()
  for (const name of plain) {
    const found = resolveName(name)
    if (!found) {
      console.error(
        `[@feldera/vite-plugin-monaco-editor] Unknown monaco feature: "${name}" — dropped.`
      )
      continue
    }
    if (seen.has(found.label)) continue
    seen.add(found.label)
    out.push(found)
  }
  if (!hasCodiconsAlias) void 0 // no-op; aliasing only meaningful when present
  return out
}

function buildFeatureRegistry(): IFeatureDefinition[] {
  const base = [...(metadata.features as IFeatureDefinition[])]
  const alreadyHasCodicons = base.some(
    (f) => f.label === 'codicons' || f.label === 'codicon'
  )
  if (!alreadyHasCodicons) {
    // Monaco <0.55 legacy file — only inject if it exists on disk.
    try {
      const p = resolveMonacoPath('vs/base/browser/ui/codicons/codiconStyles')
      if (existsSync(p)) {
        base.push({
          label: 'codicons',
          entry: 'vs/base/browser/ui/codicons/codiconStyles'
        } as IFeatureDefinition)
      }
    } catch {
      // ignore
    }
  }
  return base
}

function collectWorkers(defs: IFeatureDefinition[]): ResolvedWorker[] {
  const seen = new Set<string>()
  const workers: ResolvedWorker[] = [
    {
      label: 'editorWorkerService',
      id: 'vs/editor/editor',
      entry: 'vs/editor/editor.worker'
    }
  ]
  seen.add(workers[0].label)
  for (const d of defs) {
    const w = (d as IFeatureDefinition & { worker?: { id: string; entry: string } }).worker
    if (!w) continue
    if (seen.has(d.label)) continue
    seen.add(d.label)
    workers.push({ label: d.label, id: w.id, entry: w.entry })
  }
  return workers
}

function flattenEntry(entry: string | string[] | undefined): string[] {
  if (!entry) return []
  return Array.isArray(entry) ? entry : [entry]
}

function generateEditorMainSource(
  languageDefs: IFeatureDefinition[],
  featureDefs: IFeatureDefinition[],
  workers: ResolvedWorker[],
  globalAPI: boolean
): string {
  const lines: string[] = []

  const workerBindings: Array<{ label: string; binding: string }> = []
  workers.forEach((w, i) => {
    const binding = `worker_${i}_${w.label.replace(/[^A-Za-z0-9_]/g, '_')}`
    const abs = resolveMonacoPath(w.entry)
    lines.push(`import ${binding} from ${JSON.stringify(abs + '?worker')};`)
    workerBindings.push({ label: w.label, binding })
  })

  lines.push('')
  lines.push('self.MonacoEnvironment = {')
  lines.push(`  globalAPI: ${globalAPI ? 'true' : 'false'},`)
  lines.push('  getWorker: function (moduleId, label) {')
  lines.push('    switch (label) {')
  for (const { label, binding } of workerBindings) {
    if (label === 'editorWorkerService') continue
    lines.push(`      case ${JSON.stringify(label)}: return new ${binding}();`)
    // Aliases used by monaco internally:
    if (label === 'typescript') {
      lines.push(`      case "javascript": return new ${binding}();`)
    }
    if (label === 'html') {
      lines.push(`      case "handlebars": return new ${binding}();`)
      lines.push(`      case "razor": return new ${binding}();`)
    }
  }
  const defaultBinding =
    workerBindings.find((b) => b.label === 'editorWorkerService')?.binding ?? workerBindings[0]?.binding
  lines.push(`      default: return new ${defaultBinding}();`)
  lines.push('    }')
  lines.push('  }')
  lines.push('};')
  lines.push('')

  for (const f of featureDefs) {
    for (const e of flattenEntry(f.entry)) {
      lines.push(`import ${JSON.stringify(resolveMonacoPath(e))};`)
    }
  }

  for (const l of languageDefs) {
    for (const e of flattenEntry(l.entry)) {
      lines.push(`import ${JSON.stringify(resolveMonacoPath(e))};`)
    }
  }

  lines.push('')
  lines.push("export * from './editor.api.js';")
  return lines.join('\n')
}

// Paths inside bare side-effect imports that represent editor contribs and
// standalone add-ons. These are the ones we want to gate through the user's
// `features` selection rather than have every language/worker file pull them
// in unconditionally.
const CONTRIB_IMPORT_RE =
  /(?:\.\.[/\\])+editor[/\\](?:contrib|standalone[/\\]browser)[/\\][^'"]+/

function stripContribSideEffectImports(
  absPath: string,
  counters: { files: number; imports: number }
): string | undefined {
  const fs = require_('node:fs') as typeof import('node:fs')
  let src: string
  try {
    src = fs.readFileSync(absPath, 'utf8')
  } catch {
    return undefined
  }
  let stripped = 0
  const replaced = src.replace(
    /^(\s*)import\s+(['"])([^'"]+)\2;?\s*$/gm,
    (match, leading, _q, spec) => {
      if (CONTRIB_IMPORT_RE.test(spec)) {
        stripped++
        return leading
      }
      return match
    }
  )
  if (stripped === 0) return undefined
  counters.files++
  counters.imports += stripped
  return replaced
}

const THROW_STUB = `
throw new Error(
  "[@feldera/vite-plugin-monaco-editor] This monaco entry point pulls in every language and feature, " +
  "bypassing the plugin's selection. Import 'monaco-editor' or 'monaco-editor/esm/vs/editor/editor.main.js' instead."
);
`

export function monaco(options: MonacoOptions = {}): Plugin {
  const { features, languages, customLanguages = [], globalAPI = false } = options

  const allFeatures = buildFeatureRegistry()

  const resolvedLanguages = resolveLanguages(languages, customLanguages)
  const resolvedFeatures = resolveFeatures(features, allFeatures)
  const resolvedWorkers = collectWorkers([...resolvedLanguages, ...resolvedFeatures])

  const trimCounters = { files: 0, imports: 0 }

  return {
    name: 'feldera:vite-plugin-monaco-editor',
    enforce: 'pre',

    buildStart() {
      trimCounters.files = 0
      trimCounters.imports = 0
    },

    buildEnd() {
      if (trimCounters.imports > 0) {
        console.log(
          `\n[@feldera/vite-plugin-monaco-editor] trimmed ${trimCounters.imports} unwanted contrib import${trimCounters.imports === 1 ? '' : 's'} across ${trimCounters.files} monaco-editor file${trimCounters.files === 1 ? '' : 's'}`
        )
      }
    },

    config(config: UserConfig) {
      config.optimizeDeps ??= {}
      config.optimizeDeps.exclude ??= []
      if (!config.optimizeDeps.exclude.includes('monaco-editor')) {
        config.optimizeDeps.exclude.push('monaco-editor')
      }
      if (Array.isArray(config.optimizeDeps.include)) {
        const before = config.optimizeDeps.include
        const after = before.filter((entry) => entry !== 'monaco-editor')
        if (after.length !== before.length) {
          console.info(
            "[@feldera/vite-plugin-monaco-editor] removed 'monaco-editor' from optimizeDeps.include"
          )
        }
        config.optimizeDeps.include = after
      }
    },

    load(id: string) {
      if (EDITOR_MAIN_RE.test(id)) {
        return generateEditorMainSource(
          resolvedLanguages,
          resolvedFeatures,
          resolvedWorkers,
          globalAPI
        )
      }
      if (EDITOR_ALL_RE.test(id) || EDCORE_MAIN_RE.test(id)) {
        return THROW_STUB
      }
      if (EDITOR_MAIN_RE.test(id)) {
        // handled above
      } else if (MONACO_SRC_RE.test(id)) {
        return stripContribSideEffectImports(id, trimCounters)
      }
      return undefined
    }
  }
}

export default monaco
