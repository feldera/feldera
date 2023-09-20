export const showOnHashPart =
  ([hash, setHash]: [string, (str: string) => void]) =>
  (target: string) => ({
    show: hash.startsWith(target),
    setShow: () => setHash('')
  })
