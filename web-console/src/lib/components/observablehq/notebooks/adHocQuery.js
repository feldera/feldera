/** @import { type Runtime, type Observer } from '@observablehq/runtime' */

function _text(Inputs) {
  return Inputs.textarea({
    rows: 4,
    minlength: 1,
    submit: true,
    placeholder: 'SQL',
    disabled: false,
    width: '100%',
    marginLeft: '24px'
  })
}

async function _result(text) {
  return (
    await fetch(`http://localhost:8080/v0/pipelines/Fraud-detection/query?sql=${text}&format=json`)
  )
    .text()
    .then((t) => t.replaceAll('}{\n', '}\n{').split('\n').slice(0, -1).map(JSON.parse))
}

function _table(Inputs, result) {
  return Inputs.table(result)
}

export default function define(runtime, observer) {
  const main = runtime.module()
  main.variable(observer('viewof text')).define('viewof text', ['Inputs'], _text)
  main.variable('text').define('text', ['Generators', 'viewof text'], (G, _) => G.input(_))
  main.variable('text').define('result', ['text'], _result)
  main.variable(observer('viewof table')).define('viewof table', ['Inputs', 'result'], _table)
  return main
}
