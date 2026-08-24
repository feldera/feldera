import { useIsEnterprise } from '$lib/compositions/useEdition.svelte'

/** Premium features are the ones the enterprise and premium editions carry. */
export const usePremiumFeatures = useIsEnterprise
