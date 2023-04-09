export const removePrefix = (value: string, prefix: string) =>
  value.startsWith(prefix) ? value.slice(prefix.length) : value
