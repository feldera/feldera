export const rgbToHex = (rgb: string) => {
  const [r, g, b] = Array.from(rgb.match(/\d+/g) ?? []).map((v) => parseInt(v))
  return '#' + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1)
}
