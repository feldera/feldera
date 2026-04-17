// @feldera/monaco-editor-vite-plugin — Vite plugin to include & bundle monaco-editor.
//
// This is a fork of @bithero/monaco-editor-vite-plugin@1.0.3 with a single
// bug fix in the config hook: the original used `filter(i => i === 'monaco-editor')`
// which kept ONLY 'monaco-editor' and dropped every other entry from the
// user's optimizeDeps.include list — the opposite of what the surrounding
// comment and log message claimed. We fix the operator to `!==`.
//
// Upstream: https://codearq.net/bithero-js/monaco-editor-vite-plugin
//
// Copyright (C) 2025-present Mai-Lapyst
//
// Licensed under the GNU Affero General Public License v3.0 or later.
// See the LICENSE file in the original package for full terms.

import type { Plugin } from 'vite'
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore — monaco-editor doesn't ship types for metadata.js; callers supply monaco-editor.
import metadata from 'monaco-editor/esm/metadata.js'
import type {
  EditorFeature,
  EditorLanguage,
  IFeatureDefinition,
  NegatedEditorFeature
} from 'monaco-editor/esm/metadata.js'
import path from 'node:path'
import fs from 'node:fs'

type Languages = '*' | 'all' | EditorLanguage[]
type Features = '*' | 'all' | ('codicons' | EditorFeature | NegatedEditorFeature)[]

export interface MonacoOptions {
  features?: Features
  languages?: Languages
  customLanguages?: IFeatureDefinition[]
  globalAPI?: boolean
}

function filterNull<T>(list: (T | null | undefined)[]): T[] {
  return list.filter(Boolean) as T[]
}

function resolveLanguages(
  languages: Languages,
  customLanguages: IFeatureDefinition[]
): IFeatureDefinition[] {
  if (languages === '*' || languages === 'all') {
    return filterNull(metadata.languages.concat(customLanguages))
  }
  if (languages.length <= 0) {
    return filterNull(customLanguages)
  }
  const langById: Record<string, IFeatureDefinition> = {}
  metadata.languages.forEach((l: IFeatureDefinition) => {
    langById[l.label] = l
  })
  function resolveLanguage(name: string): IFeatureDefinition | null {
    const lang = langById[name]
    if (!lang) {
      console.error('[feldera-monaco] unknown language:', name)
      return null
    }
    return lang
  }
  return filterNull(languages.map(resolveLanguage).concat(customLanguages))
}

function resolveFeatures(features: Features | undefined): IFeatureDefinition[] {
  if (!features) {
    return metadata.features
  }
  if (features === '*' || features === 'all') {
    return metadata.features
  }
  const featureById: Record<string, IFeatureDefinition> = {}
  metadata.features.forEach((f: IFeatureDefinition) => {
    if (featureById[f.label]) {
      const def = featureById[f.label]!
      if (typeof def.entry === 'string') {
        def.entry = [def.entry]
      }
      ;(def.entry as string[]).push(...(f.entry as string[]))
    } else {
      featureById[f.label] = f
    }
  })
  // Monaco versions before 55.0 stored codicons differently.
  const codiconsPath = resolveMonacoPath('vs/base/browser/ui/codicons/codiconStyles.js')
  if (fs.existsSync(codiconsPath)) {
    featureById['codicons'] = {
      label: 'codicons',
      entry: 'vs/base/browser/ui/codicons/codiconStyles.js'
    }
  }
  function resolveFeature(name: string): IFeatureDefinition | null {
    const feature = featureById[name]
    if (!feature) {
      if (name === 'codicons' && featureById['codicon']) return featureById['codicon']!
      if (name === 'codicon' && featureById['codicons']) return featureById['codicons']!
      console.error('[feldera-monaco] unknown feature:', name)
      return null
    }
    return feature
  }
  const excluded = features.filter((f) => f[0] === '!').map((f) => f.slice(1))
  if (excluded.length > 0) {
    return filterNull(
      Object.keys(featureById)
        .filter((f) => !excluded.includes(f))
        .map(resolveFeature)
    )
  }
  return filterNull(features.map(resolveFeature))
}

const editorModule: IFeatureDefinition = {
  label: 'editorWorkerService',
  entry: undefined as unknown as string,
  worker: {
    id: 'vs/editor/editor',
    entry: 'vs/editor/editor.worker'
  }
}

interface Worker {
  label: string
  id: string
  entry: string
}

function resolveWorkers(
  languages: IFeatureDefinition[],
  features: IFeatureDefinition[]
): Worker[] {
  const modules = [editorModule].concat(languages).concat(features)
  const workers: Worker[] = []
  modules.forEach((mod) => {
    if (mod.worker) {
      workers.push({
        label: mod.label,
        id: mod.worker.id,
        entry: mod.worker.entry
      })
    }
  })
  return workers
}

function resolveModule(file: string): string {
  const url = import.meta.resolve(file).toString()
  return url.replace(/^file:\/\//, '')
}

function resolveMonacoPath(file: string): string {
  try {
    return resolveModule(path.join('monaco-editor/esm', file))
  } catch {
    /* fall through */
  }
  try {
    return resolveModule(path.join(process.cwd(), 'node_modules/monaco-editor/esm', file))
  } catch {
    /* fall through */
  }
  return resolveModule(file)
}

export function monaco(options?: MonacoOptions): Plugin {
  const languages = resolveLanguages(options?.languages || [], options?.customLanguages || [])
  const features = resolveFeatures(options?.features)
  const workers = resolveWorkers(languages, features)
  return {
    name: 'feldera-monaco',
    enforce: 'pre',
    config(config) {
      if (!config.optimizeDeps) {
        config.optimizeDeps = {}
      }
      const optimizeDeps = config.optimizeDeps
      if (!optimizeDeps.exclude) {
        optimizeDeps.exclude = []
      }
      optimizeDeps.exclude.push('monaco-editor')
      if (optimizeDeps.include) {
        // FIX (vs. upstream @bithero/monaco-editor-vite-plugin@1.0.3):
        // Original code used `filter(i => i === 'monaco-editor')` which kept
        // ONLY 'monaco-editor' and dropped every other entry the user had
        // deliberately added to the include list. The operator was inverted.
        optimizeDeps.include = optimizeDeps.include.filter((i) => i !== 'monaco-editor')
      }
    },
    load(id) {
      if (id.match(/esm[/\\]vs[/\\]editor[/\\]editor.main.js/)) {
        const workerPaths = workers.map(
          (worker) => `"${worker.label}": () => new ${worker.label}()`
        )
        const workerPathsJson = '{' + workerPaths.join(',') + '}'
        const result = [
          `// Generated by @feldera/monaco-editor-vite-plugin`,
          `// SPDX-License-Identifier: AGPL-3.0-or-later`,
          ...workers.map(
            (worker) => `import ${worker.label} from '${resolveMonacoPath(worker.entry)}?worker'`
          ),
          `self['MonacoEnvironment'] = (function (paths) {
              return {
                  globalAPI: ${options?.globalAPI || false},
                  getWorker: function (moduleId, label) {
                      var result = paths[label];
                      return result();
                  },
              };
          })(${workerPathsJson});`,
          ...features
            .flatMap((feature) => feature.entry)
            .filter((entry): entry is string => !!entry)
            .map((entry) => `import "${resolveMonacoPath(entry)}";`),
          ...languages
            .flatMap((lang) => lang.entry)
            .filter((entry): entry is string => !!entry)
            .map((entry) => `import "${resolveMonacoPath(entry)}";`),
          "export * from './editor.api.js';"
        ].join('\n')
        return result
      } else if (id.match(/esm[/\\]vs[/\\]editor[/\\]editor.all.js/)) {
        return 'throw "Please use esm/vs/editor.main.js or monaco-editor directly instead!"'
      } else if (id.match(/esm[/\\]vs[/\\]editor[/\\]edcore.main.js/)) {
        return 'throw "Please use esm/vs/editor.main.js or monaco-editor directly instead!"'
      }
      return undefined
    }
  }
}
