import { useLocalStorage } from '$lib/compositions/localStore.svelte'
import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'

export const useLayoutSettings = () => {
  const isTablet = useIsTablet()
  const showPipelinesPanel = useLocalStorage(
    'layout/pipelines/pipelinesPanel/show',
    !isTablet.current
  ) // Make pipeline drawer open by default on larger screens
  const showMonitoringPanel = useLocalStorage('layout/pipelines/monitoringPanel', true)
  const showInteractionPanel = useLocalStorage('layout/pipelines/interactionPanel', false)
  const hideWarnings = useLocalStorage('layout/pipelines/logs/hideWarnings', false)
  const verbatimErrors = useLocalStorage('layout/pipelines/logs/verbatimErrors', false)

  return {
    showPipelinesPanel,
    showMonitoringPanel,
    showInteractionPanel,
    hideWarnings,
    verbatimErrors
  }
}
