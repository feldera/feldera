import { ProjectDescr } from './manager'

export interface Schema {
  inputs: [{ name: string; fields: [{ name: string; type: string; nullable: boolean }] }]
  outputs: [{ name: string; fields: [{ name: string; type: string; nullable: boolean }] }]
}

export interface ProjectWithSchema {
  project_id: number
  name: string
  schema: Schema
}

export function parseProjectSchema(project: ProjectDescr): ProjectWithSchema {
  return {
    name: project.name,
    project_id: project.project_id,
    schema: JSON.parse(project.schema || '{ "inputs": [], "outputs": [] }')
  }
}
