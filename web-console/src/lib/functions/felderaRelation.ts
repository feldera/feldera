import { Field, Relation } from '$lib/services/manager'
import invariant from 'tiny-invariant'

export type CaseDependentName = {
  case_sensitive?: boolean | undefined
  name: string
}

export const quotifyRelationName = (relation: Relation) =>
  relation.case_sensitive ? `"${relation.name}"` : relation.name

export const caseDependentName = (quotedRelationName: string) => {
  const name = /\"?(\w+)\"?/.exec(quotedRelationName)?.[1]
  invariant(name, `Invalid relation name parsed: ${name}`)
  return {
    name,
    case_sensitive: /\"/.test(quotedRelationName)
  }
}

export const caseDependentNameEq = (a: CaseDependentName) => (b: CaseDependentName) =>
  !a.case_sensitive === !b.case_sensitive &&
  (a.case_sensitive ? a.name === b.name : a.name.toLocaleLowerCase() === b.name.toLocaleLowerCase())

export const quotifyFieldName = (field: Field) => (field.case_sensitive ? `"${field.name}"` : field.name)

export const escapeRelationName = encodeURI
export const unescapeRelationName = decodeURI
