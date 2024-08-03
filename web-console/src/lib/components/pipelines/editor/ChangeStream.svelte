<script lang="ts">
  import { page } from '$app/stores'
  import { relationEggressStream } from '$lib/services/pipelineManager'
  import type BigNumber from 'bignumber.js'
  import JSONbig from 'true-json-bigint'

  let { pipelineName, relationName }: { pipelineName: string; relationName: string } = $props()

  let rows = $state<Record<string, Record<'insert' | 'delete', XgressRecord>[]>>({
    [pipelineName]: []
  }) // Initialize row array
  $effect(() => {
    console.log('entered small effect', pipelineName)
    // Initialize row array when pipelineName changes
    rows[pipelineName] ??= []
  })

  // const stream = $derived(relationEggressStream(pipelineName, relationName, $page.data.auth === 'none' ? undefined : $page.data.auth.accessToken))

  $effect(() => {
    console.log('entered effect', pipelineName, relationName)
    // const abortController = new AbortController()
    const x = relationEggressStream(
      pipelineName,
      relationName,
      undefined, //abortController.signal,
      $page.data.auth === 'none' ? undefined : $page.data.auth.accessToken
    ).then((stream) => {
      if (!stream) {
        return undefined
      }
      console.log('entering!', pipelineName, relationName)
      const rd = stream.getReader()
      accumulateRows(pipelineName, rd)
      return () => rd.cancel('not_needed')
    })
    return () => {
      x.then((cancel) => cancel?.())
      // abortController.abort('not_needed')
    }
  })

  const decoder = new TextDecoder()

  type XgressRecord = Record<string, string | number | boolean | BigNumber | Date>

  const accumulateRows = async (
    pipelineName: string,
    reader: ReadableStreamDefaultReader<Uint8Array>
  ) => {
    while (true) {
      const { done, value } = await reader.read().catch((e) => {
        console.log('stream err', e)
        // if (e instanceof DOMException && e.message === 'BodyStreamBuffer was aborted') {
        //   return {
        //     done: true,
        //     value: undefined
        //   }
        // }
        throw e
      })
      if (done) {
        console.log('done')
        break
      }

      const chunk = decoder.decode(value, { stream: true })
      const strings = chunk.split(/\r?\n/)

      for (const str of strings) {
        if (str.trim() === '') {
          continue // Add only non-empty strings
        }
        const obj: {
          sequence_number: number
          json_data?: Record<'insert' | 'delete', XgressRecord>[]
        } = JSONbig.parse(str)
        if (!obj.json_data) {
          console.log('NO MORE NO MORE NO MORE')
          continue
        }
        rows[pipelineName].splice(100 - obj.json_data.length)
        rows[pipelineName].unshift(...obj.json_data.reverse())
        // rows[pipelineName].splice(100 - obj.json_data.length, 100, ...obj.json_data)
      }
    }
  }

  import { VList } from 'virtua/svelte'
</script>

<div class="flex-1">
  <VList data={rows[pipelineName] ?? []} let:item getKey={(d, i) => i}>
    <div
      class={'border-l-4 pl-2 even:bg-surface-100-900 ' +
        ('insert' in item
          ? '  border-green-500 '
          : 'delete' in item
            ? 'border-l-4 border-red-500'
            : '')}
    >
      {JSONbig.stringify(item.insert ?? item.delete)}
    </div>
  </VList>
</div>
