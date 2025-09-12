import { error } from '@sveltejs/kit'

// This layout file is used to capture and display critical errors, forwarded from the root layout, while having the latest page data properly initialized.
// This is impossible to achieve by emitting errors in the root layout since then the latest page data is not initialized (because a normal return from the `load` function does not happen).

export const load = async ({ parent }) => {
  const data = await parent()
  if (data.error) {
    error(401, data.error.message)
  }
  return data
}
