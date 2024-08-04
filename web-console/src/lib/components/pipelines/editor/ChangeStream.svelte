<script lang="ts" context="module">
  type XgressRecord = Record<string, string | number | boolean | BigNumber | Date>

  let rows = $state<Record<string, Record<'insert' | 'delete', XgressRecord>[]>>({}) // Initialize row array
</script>

<script lang="ts">
  import { relationEggressStream } from '$lib/services/pipelineManager'
  import type BigNumber from 'bignumber.js'
  import JSONbig from 'true-json-bigint'

  import { VList } from 'virtua/svelte'

  let { pipelineName, relationName }: { pipelineName: string; relationName: string } = $props()

  $effect(() => {
    // Initialize row array when pipelineName changes
    rows[pipelineName] ??= []
  })

  $effect(() => {
    const handle = relationEggressStream(pipelineName, relationName).then((stream) => {
      if (!stream) {
        return undefined
      }
      console.log('entering!', pipelineName, relationName)
      const rd = stream.getReader()
      accumulateRows(pipelineName, rd)
      return () => rd.cancel('not_needed')
    })
    return () => {
      handle.then((cancel) => cancel?.())
    }
  })

  const decoder = new TextDecoder()

  const accumulateRows = async (
    pipelineName: string,
    reader: ReadableStreamDefaultReader<Uint8Array>
  ) => {
    const bufferSize = 100
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
        rows[pipelineName].splice(Math.max(bufferSize - obj.json_data.length, 0))
        rows[pipelineName].unshift(...obj.json_data.slice(-bufferSize).reverse())
      }
    }
  }
</script>

<div class="flex-1">
  <!--
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
  -->
  <div class="h-full overflow-auto">
    {#each rows[pipelineName] as item}
      <div
        class={'even:bg-surface-100-900 border-l-4 pl-2 even:!bg-opacity-30 ' +
          ('insert' in item
            ? '  border-green-500 '
            : 'delete' in item
              ? 'border-l-4 border-red-500'
              : '')}>
        {JSONbig.stringify(item.insert ?? item.delete)}
      </div>
    {/each}
  </div>
</div>
