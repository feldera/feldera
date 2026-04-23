<script lang="ts">
  import { triggerOidcLogin } from '$lib/services/auth'
</script>

<span>
  Failed to authenticate to this Feldera instance. Try to
  <button type="button" class="underline" onclick={() => triggerOidcLogin()}> log in </button>.
</span>
