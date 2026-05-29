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
  /**
   * Emit worker imports as `?worker&inline` so workers are bundled as base64
   * data URIs in the main chunk instead of separate sibling files. Use this for
   * single-file builds (e.g. `vite-plugin-singlefile`) that need a self-contained
   * artifact. Default `false` — separate worker files are preferable for normal
   * apps since they can be loaded lazily per language.
   */
  inlineWorkers?: boolean
}

interface ResolvedWorker {
  label: string
  id: string
  entry: string
}

const EDITOR_MAIN_RE = /monaco-editor[/\\]esm[/\\]vs[/\\]editor[/\\]editor\.main\.js$/
const EDITOR_ALL_RE = /monaco-editor[/\\]esm[/\\]vs[/\\]editor[/\\]editor\.all\.js$/
const EDCORE_MAIN_RE = /monaco-editor[/\\]esm[/\\]vs[/\\]editor[/\\]edcore\.main\.js$/
// Advanced-language-service modules live under `vs/language/<dir>/`, where
// `<dir>` matches one of the language entries declared in monaco's metadata
// (json, typescript, css, html). Each is a self-contained bundle (worker,
// monaco.contribution, mode) that is only useful when the consumer enables
// that language. Consumer code that dynamic-imports one of these (e.g. for
// JSON-schema validation) must be neutered when the language is not in the
// configured set; otherwise rollup's `inlineDynamicImports` drags the entire
// dir — and its ~400 KB worker — into the bundle. Subdirs like
// `vs/language/common/` are shared infrastructure, NOT languages, and must
// pass through (gated below by `allLanguageDirs`).
const LANGUAGE_DIR_RE = /monaco-editor[/\\]esm[/\\]vs[/\\]language[/\\]([^/\\]+)[/\\]/
// Basic-language files (`vs/basic-languages/<x>/`) are tokenizer-only monarch
// grammars — they don't depend on editor contributions to function. Monaco
// nonetheless ships them with bare `import "../editor/contrib/.../X.js"`
// side-effect imports that pull every contrib into the bundle, defeating the
// `features` selection emitted by the editor.main.js replacement. Strip those
// here. LSP languages (`vs/language/<x>/`) get the opposite treatment: their
// modes legitimately need the contribs they import (snippet, semanticTokens,
// documentSymbols, codeAction, inlineCompletions, …) for schema-based
// IntelliSense to work, so we leave their imports untouched.
const BASIC_LANGUAGE_SRC_RE = /monaco-editor[/\\]esm[/\\]vs[/\\]basic-languages[/\\].+\.js$/

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
  globalAPI: boolean,
  inlineWorkers: boolean
): string {
  const lines: string[] = []

  const workerQuery = inlineWorkers ? '?worker&inline' : '?worker'
  const workerBindings: Array<{ label: string; binding: string }> = []
  workers.forEach((w, i) => {
    const binding = `worker_${i}_${w.label.replace(/[^A-Za-z0-9_]/g, '_')}`
    const abs = resolveMonacoPath(w.entry)
    lines.push(`import ${binding} from ${JSON.stringify(abs + workerQuery)};`)
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
// standalone add-ons. These get stripped from basic-language source files
// (see BASIC_LANGUAGE_SRC_RE); LSP languages retain their contrib imports.
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
  const {
    features,
    languages,
    customLanguages = [],
    globalAPI = false,
    inlineWorkers = false
  } = options

  const allFeatures = buildFeatureRegistry()

  const resolvedLanguages = resolveLanguages(languages, customLanguages)
  const resolvedFeatures = resolveFeatures(features, allFeatures)
  const resolvedWorkers = collectWorkers([...resolvedLanguages, ...resolvedFeatures])

  // Every `vs/language/<dir>/` subdir referenced by any monaco language in the
  // full metadata. This is the universe of "advanced language services" the
  // plugin might suppress — anything outside this set (e.g. `vs/language/common/`,
  // which is shared infrastructure used by typescript/json/css/html) must pass
  // through untouched.
  const allLanguageDirs = new Set<string>()
  for (const def of metadata.languages as IFeatureDefinition[]) {
    for (const entry of flattenEntry(def.entry)) {
      const m = entry.match(/vs[/\\]language[/\\]([^/\\]+)[/\\]/)
      if (m) allLanguageDirs.add(m[1])
    }
  }
  const enabledLanguageDirs = new Set<string>()
  for (const def of resolvedLanguages) {
    for (const entry of flattenEntry(def.entry)) {
      const m = entry.match(/vs[/\\]language[/\\]([^/\\]+)[/\\]/)
      if (m) enabledLanguageDirs.add(m[1])
    }
  }
  const warnedLanguageDirs = new Set<string>()

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
          `\n[@feldera/vite-plugin-monaco-editor] trimmed ${trimCounters.imports} unwanted contrib import${trimCounters.imports === 1 ? '' : 's'} across ${trimCounters.files} basic-language file${trimCounters.files === 1 ? '' : 's'}`
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
          globalAPI,
          inlineWorkers
        )
      }
      if (EDITOR_ALL_RE.test(id) || EDCORE_MAIN_RE.test(id)) {
        return THROW_STUB
      }
      const langMatch = id.match(LANGUAGE_DIR_RE)
      if (
        langMatch &&
        allLanguageDirs.has(langMatch[1]) &&
        !enabledLanguageDirs.has(langMatch[1])
      ) {
        if (!warnedLanguageDirs.has(langMatch[1])) {
          warnedLanguageDirs.add(langMatch[1])
          console.warn(
            `[@feldera/vite-plugin-monaco-editor] Suppressed import of disabled language "${langMatch[1]}" — add it to the plugin's \`languages\` option to enable.`
          )
        }
        return 'export {}'
      }
      if (BASIC_LANGUAGE_SRC_RE.test(id)) {
        return stripContribSideEffectImports(id, trimCounters)
      }
      return undefined
    }
  }
}

export default monaco
