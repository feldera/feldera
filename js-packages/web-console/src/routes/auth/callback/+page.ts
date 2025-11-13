export const load = async ({ url, parent }): Promise<void> => {
  await parent()

  // This route is used by @axa-fr/oidc-client to handle the OIDC redirect callback.
  // The redirection will happen automatically, this load function only prevents a temporary 404 error.
}
