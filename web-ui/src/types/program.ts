import { ProgramDescr } from './manager'

export interface Schema {
  inputs: [{ name: string; fields: [{ name: string; type: string; nullable: boolean }] }]
  outputs: [{ name: string; fields: [{ name: string; type: string; nullable: boolean }] }]
}

export interface ProjectWithSchema {
  program_id: string
  name: string
  schema: Schema
}

export function parseProjectSchema(project: ProgramDescr): ProjectWithSchema {
  return {
    name: project.name,
    program_id: project.program_id,
    schema: JSON.parse(project.schema || '{ "inputs": [], "outputs": [] }')
  }
}
