// Remove a prefix in a string (if it exists)
export const removePrefix = (value: string, prefix: string) =>
  value.startsWith(prefix) ? value.slice(prefix.length) : value
