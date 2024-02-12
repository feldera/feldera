export type CaseDependentName = {
  case_sensitive?: boolean
  name: string
}

export const getCaseIndependentName = (entity: CaseDependentName) =>
  entity.case_sensitive ? `"${entity.name}"` : entity.name

// TODO: update implementation when double quotes become a legal symbol in a name
export const getCaseDependentName = (caseIndependentName: string) => {
  return {
    name: caseIndependentName.replaceAll('"', ''),
    case_sensitive: caseIndependentName.includes('"')
  }
}

export const caseDependentNameEq = (a: CaseDependentName) => (b: CaseDependentName) =>
  !a.case_sensitive === !b.case_sensitive &&
  (a.case_sensitive ? a.name === b.name : a.name.toLocaleLowerCase() === b.name.toLocaleLowerCase())

export const escapeRelationName = encodeURI
export const unescapeRelationName = decodeURI
