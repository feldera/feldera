/**
 * The page a redirect interrupted, so whatever interrupted it can send the user
 * back. Two flows share this: the OIDC login redirect and the acting-tenant
 * gate. Both take the user away from a page they asked for and owe them a way
 * back to it.
 *
 * Session storage rather than a query parameter: the target survives the
 * provider's cross-origin round trip, and it stays out of URLs the user might
 * share or bookmark.
 *
 * Only the first write is kept, because a fallback navigation may re-enter the
 * same flow from `/` and must not overwrite the page originally asked for.
 * Reading takes the value: whoever restores it also consumes it, so a later
 * redirect cannot land on a stale target.
 */
const REDIRECT_TARGET_KEY = 'redirect_to'

export const stashRedirectTarget = (href: string) => {
  if (!window.sessionStorage.getItem(REDIRECT_TARGET_KEY)) {
    window.sessionStorage.setItem(REDIRECT_TARGET_KEY, href)
  }
}

export const takeRedirectTarget = (): string | undefined => {
  const href = window.sessionStorage.getItem(REDIRECT_TARGET_KEY)
  if (href) {
    window.sessionStorage.removeItem(REDIRECT_TARGET_KEY)
  }
  return href ?? undefined
}
