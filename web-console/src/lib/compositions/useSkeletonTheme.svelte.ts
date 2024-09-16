let state = $state(document.body.getAttribute('data-theme') ?? '')

const config = $derived({
  monospaceFontFamily: (() => {
    switch (state) {
      case 'feldera-classic-theme':
        return 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace'
      case 'feldera-modern-theme':
        return 'DM Mono'
      default:
        return ''
    }
  })()
})

const skeletonTheme = {
  get current() {
    return state
  },
  get config() {
    return config
  },
  set current(name: string) {
    document.body.setAttribute('data-theme', name)
    state = name
  }
}

export const useSkeletonTheme = () => skeletonTheme
