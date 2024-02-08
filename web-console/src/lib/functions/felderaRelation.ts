import { Field, Relation } from '$lib/services/manager'

export type CaseDependentName = {
  case_sensitive?: boolean
  name: string
}

export const quotifyRelationName = (relation: Relation) =>
  relation.case_sensitive ? `"${relation.name}"` : relation.name

export const caseDependentName = (quotedRelationName: string) => {
  return {
    name: quotedRelationName.replaceAll('"', ''),
    case_sensitive: quotedRelationName.includes('"')
  }
}

export const caseDependentNameEq = (a: CaseDependentName) => (b: CaseDependentName) =>
  !a.case_sensitive === !b.case_sensitive &&
  (a.case_sensitive ? a.name === b.name : a.name.toLocaleLowerCase() === b.name.toLocaleLowerCase())

export const quotifyFieldName = (field: Field) => (field.case_sensitive ? `"${field.name}"` : field.name)

export const escapeRelationName = encodeURI
export const unescapeRelationName = decodeURI
