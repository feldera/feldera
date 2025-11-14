import args from 'args'
import { $ } from 'bun'
import webfontsGenerator from '@vusion/webfonts-generator'
import fs from 'node:fs/promises'

args
  .option('src', 'Directory to read all SVGs from')
  .option('dest', 'Where to place webfont files. Overwrites existing duplicates')
  .option('name', 'The name of the result webfont')
  .option('prefix', 'The name of the result webfont')
  .option('fix', 'Simplify the SVG to only contain filled path elements')

const { src, dest, name, prefix, fix } = args.parse(process.argv)

const svgDir = fix
  ? await (async () => {
      const tmp = `tmp/build-webfont/${name}`
      await $`mkdir -p ${tmp}`
      await $`oslllo-svg-fixer -s ${src} -d ${tmp}`
      return tmp
    })()
  : src
const fileNames = await fs.readdir(svgDir)
webfontsGenerator(
  {
    files: fileNames.map((file) => `${svgDir}/${file}`),
    dest: dest,
    fontName: name,
    templateOptions: {
      baseSelector: `.${prefix}`,
      classPrefix: `${prefix}-`
    },
    types: ['woff2', 'woff', 'svg'],
    order: ['woff2', 'woff'],
    formatOptions: {
      svg: {
        fontHeight: 128
      }
    }
  },
  function (error: any) {
    if (error) {
      console.log('Failed to build webfont!', error)
      process.exitCode = 1
    } else {
      console.log(`Generated font ${name}`)
    }
  }
)
