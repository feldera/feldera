export const load = async ({ parent }) => {
  await parent()
  return {}
}
