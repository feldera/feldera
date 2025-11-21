export const useDownloadProgress = () => {
  let bytes = $state({
    downloaded: 0,
    total: 0
  })
  let percent = $derived(bytes.total ? (bytes.downloaded / bytes.total) * 100 : null)
  return {
    get percent() {
      return percent
    },
    get bytes() {
      return bytes
    },
    onProgress: (downloaded: number, total: number) => {
      bytes = { downloaded, total }
    },
    reset: () => {
      bytes = {
        downloaded: 0,
        total: 0
      }
    }
  }
}
