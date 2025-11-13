export function scrollStop(refresh = 100) {
  let isScrolling: ReturnType<typeof setTimeout>

  return (callback: () => void) => {
    clearTimeout(isScrolling)

    isScrolling = setTimeout(callback, refresh)
  }
}

export function scrollSpeed(refresh = 200) {
  let lastScrollPosition: number | undefined = undefined
  let isScrollingFast: ReturnType<typeof setTimeout> | undefined

  return (
      speed: number,
      callback: {
        fast: () => void
        slow: () => void
      }
    ) =>
    (scrollPosition: number) => {
      if (!lastScrollPosition) {
        lastScrollPosition = scrollPosition
      } else {
        if (Math.abs(scrollPosition - lastScrollPosition) > speed) {
          callback.fast()

          if (isScrollingFast !== undefined) {
            clearTimeout(isScrollingFast)

            isScrollingFast = undefined
          }

          isScrollingFast = setTimeout(() => {
            callback.slow()

            isScrollingFast = undefined
          }, refresh)
        } else {
          if (isScrollingFast === undefined) {
            callback.slow()
          }
        }

        lastScrollPosition = scrollPosition
      }
    }
}

export const round = {
  ceil: (x: number, multiple: number) => Math.ceil(x / multiple) * multiple,
  floor: (x: number, multiple: number) => ~~(x / multiple) * multiple
}

export const getGridIndices = (
  itemCount: number,
  itemHeight: number,
  height: number,
  columnCount: number,
  overScanColumn: number,
  scrollPosition: number
): number[] => {
  const indices: number[] = []

  const startIndexTemp = round.floor((scrollPosition / itemHeight) * columnCount, columnCount)
  const startIndexOverScan = startIndexTemp > overScanColumn ? startIndexTemp - overScanColumn : 0
  const startIndex =
    startIndexTemp > 0 && startIndexOverScan >= 0 ? startIndexOverScan : startIndexTemp

  const endIndexTemp = Math.min(
    itemCount,
    round.ceil(((scrollPosition + height) / itemHeight) * columnCount, columnCount)
  )
  const endIndexOverScan = endIndexTemp + overScanColumn
  const endIndex = endIndexOverScan < itemCount ? endIndexOverScan : itemCount

  for (let i = startIndex; i < endIndex; i++) indices.push(i)

  return indices
}

export const getListIndices = (
  itemCount: number,
  itemSize: number,
  size: number,
  overScan: number,
  scrollPosition: number
): number[] => {
  const indices: number[] = []

  const startIndexTemp = ~~(scrollPosition / itemSize)
  const startIndexOverScan = startIndexTemp > overScan ? startIndexTemp - overScan : 0
  const startIndex = startIndexOverScan >= 0 ? startIndexOverScan : startIndexTemp

  const endIndexTemp = Math.min(itemCount, ~~((scrollPosition + size) / itemSize))
  const endIndexOverScan = endIndexTemp + overScan
  const endIndex = endIndexOverScan < itemCount ? endIndexOverScan : itemCount

  for (let i = startIndex; i < endIndex; i++) indices.push(i)

  return indices
}

export const getRowIndex = (index: number, columnCount: number) => ~~(index / columnCount)
