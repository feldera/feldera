<script lang="ts">
  import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
  import type { Snippet } from 'svelte'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import type { LayoutData } from './$types'
  import {
    usePipelineList,
    useRefreshPipelineList
  } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { SvelteKitTopLoader } from 'sveltekit-top-loader'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'
  import ModalDrawer from '$lib/components/layout/ModalDrawer.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import PipelineList from '$lib/components/pipelines/List.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'

  const dialog = useGlobalDialog()

  let { children, data }: { children: Snippet; data: LayoutData } = $props()

  useRefreshPipelineList()
  const rightDrawer = useDrawer('right')
  const isTablet = useIsTablet()
  const leftDrawer = useLocalStorage('layout/pipelines/pipelinesPanel/show', !isTablet.current) // Make pipeline drawer open by default on larger screens
  const pipelineList = usePipelineList(data.preloaded)
</script>

<SvelteKitTopLoader height={2} color={'rgb(var(--color-primary-500))'} showSpinner={false}
></SvelteKitTopLoader>
<div class="flex h-full w-full justify-center">
  <!-- <Drawer width="w-[22rem]" bind:open={showDrawer.value} side="left">
    <div class="flex h-full w-full flex-col gap-1">
      <span class="mx-5 my-4 flex items-end justify-center">
        <a href="{base}/">
          {#if darkMode.value === 'dark'}
            <FelderaModernLogoColorLight class="h-12"></FelderaModernLogoColorLight>
          {:else}
            <FelderaModernLogoColorDark class="h-12"></FelderaModernLogoColorDark>
          {/if}
        </a>
      </span>
      <PipelinesList bind:pipelines={pipelines.pipelines}></PipelinesList>
    </div>
  </Drawer> -->
  <div class="flex h-full w-full flex-col">
    {@render children()}
  </div>
  {#if isTablet.current}
    <ModalDrawer
      width="w-72"
      bind:open={leftDrawer.value}
      side="left"
      class="bg-white-dark flex flex-col gap-2 pl-4 pr-1 pt-8"
    >
      <PipelineList
        pipelines={pipelineList.pipelines}
        onclose={() => {
          leftDrawer.value = false
        }}
        onaction={() => {
          leftDrawer.value = false
        }}
      ></PipelineList>
    </ModalDrawer>
  {/if}
  <ModalDrawer
    width="w-72"
    bind:open={rightDrawer.value}
    side="right"
    class="bg-white-dark flex flex-col gap-2 p-4"
  >
    <div class="relative my-2 mt-4">
      <CreatePipelineButton
        btnClass="preset-filled-surface-50-950"
        onSuccess={() => {
          rightDrawer.value = false
        }}
      ></CreatePipelineButton>
    </div>
    <BookADemo class="self-center preset-filled-primary-500">Book a demo</BookADemo>
    <NavigationExtras inline></NavigationExtras>
  </ModalDrawer>
</div>
<GlobalModal dialog={dialog.dialog} onClose={() => (dialog.dialog = null)}></GlobalModal>
