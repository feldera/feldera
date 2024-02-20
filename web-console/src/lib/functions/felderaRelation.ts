export type CaseDependentName = {
  case_sensitive?: boolean
  name: string
}

export const getCaseIndependentName = (entity: CaseDependentName) =>
  entity.case_sensitive ? `"${entity.name}"` : entity.name

const isCaseSensitive = (caseIndependentName: string) => caseIndependentName.includes('"')

/**
 * Convert case insensitive names to lowercase, leave names in quotes as-is
 */
export const normalizeCaseIndependentName = (
  caseIndependentName: string,
  caseSensitive = isCaseSensitive(caseIndependentName)
) => (caseSensitive ? caseIndependentName : caseIndependentName.toLocaleLowerCase())

// TODO: update implementation when double quotes become a legal symbol in a name
export const getCaseDependentName = (caseIndependentName: string) => {
  const caseSensitive = isCaseSensitive(caseIndependentName)
  return {
    name: normalizeCaseIndependentName(caseIndependentName.replaceAll('"', ''), caseSensitive),
    case_sensitive: caseSensitive
  }
}

export const caseDependentNameEq = (a: CaseDependentName) => (b: CaseDependentName) =>
  !a.case_sensitive === !b.case_sensitive &&
  (a.case_sensitive ? a.name === b.name : a.name.toLocaleLowerCase() === b.name.toLocaleLowerCase())

export const escapeRelationName = (name: CaseDependentName) => encodeURI(getCaseIndependentName(name))
export const unescapeRelationName = decodeURI
