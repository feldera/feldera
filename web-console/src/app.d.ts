import '@poppanator/sveltekit-svg/dist/svg'
import type { LayoutData } from './routes/+layout.js'

// See https://kit.svelte.dev/docs/types#app
// for information about these interfaces
declare global {
  namespace App {
    // interface Error {}
    // interface Locals {}
    interface PageData extends LayoutData {}
    // interface PageState {}
    // interface Platform {}
  }
}

export {}
