// Check if a string is a valid URL
//
// See: https://stackoverflow.com/questions/61634973/yup-validation-of-website-using-url-very-strict
export const isUrl = (url: string | undefined) => {
  if (!url) return false
  try {
    new URL(url)
  } catch (e) {
    return false
  }

  return true
}
