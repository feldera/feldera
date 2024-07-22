<script lang="ts">
  import { base } from '$app/paths'
  import { page } from '$app/stores'
  import type { User } from '@auth/core/types'
  import { signOut } from '@auth/sveltekit/client'
  import invariant from 'tiny-invariant'

  let { user }: { user: User } = $props()
</script>

<div class="flex flex-col gap-4 p-4">
  <div>
    <div class="flex gap-2">
      {#if user.image}
        <img class="h-10 w-10 rounded-full" src={user.image} alt="User avatar" />
      {:else}
        <div class="bg-surface-500 h-10 w-10 rounded-full"></div>
      {/if}
      <div>
        <div class="h4 font-normal" class:italic={!user.name}>{user.name || 'anonymous'}</div>
        <div class="">{user.email}</div>
      </div>
      <div class="text-surface-600-400 ml-auto">logged in</div>
    </div>
  </div>
  <div class="hr"></div>
  <div>
    <button
      class="btn text-surface-800-200 preset-outlined-surface-50-950 hover:preset-filled-surface-50-950"
      onclick={async () => {
        invariant($page.data.authDetails.enabled === true)
        await signOut({
          callbackUrl: $page.data.authDetails.providerSignOutUrl.replaceAll(
            '{redirect_uri}',
            encodeURIComponent(window.location.origin + base)
          )
        })
        // await signOut({ callbackUrl: 'https://dev-jzraqtxsr8a3hhhv.us.auth0.com/oidc/logout' })
      }}><span class="bx bx-right-arrow-alt text-[24px]"></span> Logout</button>
  </div>
</div>
