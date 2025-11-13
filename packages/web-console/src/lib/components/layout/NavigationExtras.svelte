<script lang="ts">
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import IconDiscord from '$assets/icons/vendors/discord-logomark-color.svg?component'
  import IconSlack from '$assets/icons/vendors/slack-logomark-color.svg?component'

  let { inline }: { inline?: boolean } = $props()

  const communityResources = [
    {
      title: 'Discord',
      path: 'https://discord.com/invite/s6t5n9UzHE',
      class: 'w-6 before:-ml-0.5',
      icon: IconDiscord,
      openInNewTab: true,
      testid: 'button-vertical-nav-discord'
    },
    {
      title: 'Slack',
      path: 'https://felderacommunity.slack.com/join/shared_invite/zt-222bq930h-dgsu5IEzAihHg8nQt~dHzA',
      class: 'w-6 before:ml-0.5',
      icon: IconSlack,
      openInNewTab: true,
      testid: 'button-vertical-nav-slack'
    }
  ]
  const docChapters = [
    {
      title: 'SQL Reference',
      href: 'https://docs.feldera.com/sql/types'
    },
    {
      title: 'Connectors',
      href: 'https://docs.feldera.com/connectors/sources/'
    },
    {
      title: 'UDFs',
      href: 'https://docs.feldera.com/sql/udf'
    },
    {
      title: 'Feldera 101',
      href: 'https://docs.feldera.com/tutorials/basics/part1'
    }
  ]
</script>

{#snippet dropdownHeader(full: string, short: string, toggle?: () => void, isOpen?: boolean)}
  <button onclick={toggle} class="btn px-1 {toggle ? '' : 'cursor-default'} ">
    {#if inline}
      {full}
    {:else}
      <span class="hidden sm:inline">{full}</span>
      <span class="inline sm:hidden">{short}</span>
      <span class="fd fd-chevron-down text-[20px] transition-transform" class:rotate-180={isOpen}
      ></span>
    {/if}
  </button>
{/snippet}

{#snippet docsButton(toggle?: () => void, isOpen?: boolean)}
  {@render dropdownHeader('Documentation', 'Docs', toggle, isOpen)}
{/snippet}

{#snippet communityButton(toggle?: () => void, isOpen?: boolean)}
  {@render dropdownHeader('Community Resources', 'Community', toggle, isOpen)}
{/snippet}

{#snippet docsItems()}
  {#each docChapters as doc}
    <a
      href={doc.href}
      target="_blank"
      rel="noreferrer"
      class="rounded p-2 hover:preset-tonal-surface">{doc.title}</a
    >
  {/each}
{/snippet}

{#snippet communityItems()}
  {#each communityResources as item}
    <a
      href={Array.isArray(item.path) ? item.path[0] : item.path}
      target="_blank"
      rel="noreferrer"
      class="flex h-10 flex-nowrap items-center gap-4 rounded p-2 hover:preset-tonal-surface"
      {...item.openInNewTab ? { target: '_blank', rel: 'noreferrer' } : undefined}
    >
      <item.icon class="{item.class} text-[20px]" />
      <!-- <div class="{item.class} text-[20px]"></div> -->
      <span class="">{item.title}</span>
    </a>
  {/each}
{/snippet}

{#if inline}
  <span class="flex flex-col text-surface-500">
    {@render docsButton()}
  </span>
  {@render docsItems()}
  <span class="flex flex-col text-surface-500">
    {@render communityButton()}
  </span>
  {@render communityItems()}
{:else}
  <Popup trigger={docsButton}>
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="bg-white-dark absolute left-0 z-30 flex max-h-[400px] w-[calc(100vw-100px)] max-w-[200px] flex-col justify-end gap-2 overflow-y-auto rounded-container p-2 shadow-md"
      >
        {@render docsItems()}
      </div>
    {/snippet}
  </Popup>
  <Popup trigger={communityButton}>
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="bg-white-dark absolute right-0 z-30 flex max-h-[400px] w-[calc(100vw-100px)] max-w-[200px] flex-col justify-end gap-2 overflow-y-auto rounded-container p-2 shadow-md"
      >
        {@render communityItems()}
      </div>
    {/snippet}
  </Popup>
{/if}
