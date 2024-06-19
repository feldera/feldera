import { handled } from "$lib/functions/request"
import { createOrReplaceProgram, deleteProgram, getPipeline, getProgram, newProgram, updatePipeline, type UpdatePipelineRequest } from "$lib/services/manager"
import { asyncWritable, readable, type Loadable, type Readable } from "@square/svelte-store"

const getPipelineAndCode = (pipeline_name: string) => handled(getPipeline)({ path: { pipeline_name } }).then(async pipeline => {
  const program = pipeline.descriptor.program_name ? await handled(getProgram)({path: {program_name: pipeline.descriptor.program_name}, query: {with_code: true}}) : undefined
  const code = program?.code ?? ''
  return { ...pipeline, code }
})

export const writablePipeline = (pipelineName: Readable<string>) => asyncWritable(
  pipelineName,
  getPipelineAndCode,
  async (newPipeline, _, oldPipeline) => {
    const program_name = (oldPipeline?.descriptor.name !== newPipeline.descriptor.name ? undefined : newPipeline.descriptor.program_name) ?? newPipeline.descriptor.name + '_program'
    if (oldPipeline?.descriptor.program_name && oldPipeline.descriptor.program_name !== program_name) {
      await deleteProgram({path: {program_name: oldPipeline.descriptor.program_name}})
    }
    await createOrReplaceProgram({body: {code: newPipeline.code, description: ''}, path: {program_name}})
    await updatePipeline({
      body: (({ pipeline_id, version, attached_connectors, ...d }) =>
        ({ ...d, connectors: attached_connectors, program_name }) satisfies UpdatePipelineRequest)(
        newPipeline.descriptor
      ),
      path: { pipeline_name: oldPipeline!.descriptor.name }
    })
    return getPipelineAndCode(newPipeline.descriptor.name)
  }
)