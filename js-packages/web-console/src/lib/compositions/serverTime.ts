let offsetMs = 0

/**
 * Sets the global time offset based on the provided server time.
 * @param newTime - A server-provided time (ISO string or Date object)
 * @returns Server time
 */
export const setCurrentTime = (newTime: string | Date) => {
  const serverMs = new Date(newTime).getTime()
  const clientMs = Date.now()
  offsetMs = serverMs - clientMs
}

export const newDate = () => new Date(Date.now() + offsetMs)

export const dateNow = () => Date.now() + offsetMs
