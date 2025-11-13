let state = $state(document.body.getAttribute('data-theme') ?? '')

const config = $derived({
  monospaceFontFamily: (() => {
    switch (state) {
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
