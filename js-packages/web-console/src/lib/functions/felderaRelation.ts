export type CaseDependentName = {
  case_sensitive?: boolean
  name: string
}

export const getCaseIndependentName = (entity: CaseDependentName) =>
  entity.case_sensitive ? `"${entity.name}"` : normalizeCaseIndependentName(entity)

const isCaseSensitive = (caseIndependentName: string) => /^"|[A-Z]/.test(caseIndependentName)

/**
 * Convert case insensitive names to lowercase, ensure case sensitive names are in quotes
 */
export const normalizeCaseIndependentName = ({
  name,
  case_sensitive = isCaseSensitive(name)
}: CaseDependentName) =>
  case_sensitive ? `"${name}"`.replaceAll('""', '"') : name.toLocaleLowerCase()

// TODO: update implementation when double quotes become a legal symbol in a name
export const getCaseDependentName = (caseIndependentName: string) => {
  const case_sensitive = isCaseSensitive(caseIndependentName)
  return {
    name: normalizeCaseIndependentName({
      name: caseIndependentName.replaceAll('"', ''),
      case_sensitive
    }),
    case_sensitive
  }
}

export const caseDependentNameEq = (a: CaseDependentName) => (b: CaseDependentName) =>
  !a.case_sensitive === !b.case_sensitive &&
  (a.case_sensitive ? a.name === b.name : a.name.toLocaleLowerCase() === b.name.toLocaleLowerCase())

export const escapeRelationName = (name: CaseDependentName) =>
  encodeURI(getCaseIndependentName(name))
export const unescapeRelationName = decodeURI
