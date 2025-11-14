<!-- Inspired from 'sveltekit-top-loader' npm package -->

<script lang="ts">
  import { run } from 'svelte/legacy'

  import { afterNavigate, beforeNavigate } from '$app/navigation'
  import nProgress from 'nprogress'
  import { onMount } from 'svelte'
  import type { AfterNavigate, BeforeNavigate, Navigation, NavigationType } from '@sveltejs/kit'

  interface Props {
    /**
     * Color for the top loader.
     * @default "#29d"
     */
    color?: string
    /**
     * The initial position for the top loader in percentage, 0.08 is 8%.
     * @default 0.08
     */
    minimum?: number
    /**
     * The increament delay speed in milliseconds.
     * @default 200
     */
    trickleSpeed?: number
    /**
     * The height for the top loader in pixels (px).
     * @default 3
     */
    height?: number
    /**
     * Auto increamenting behaviour for the top loader.
     * @default true
     */
    trickle?: boolean
    /**
     * To show spinner or not.
     * @default true
     */
    showSpinner?: boolean
    /**
     * Animation settings using easing (a CSS easing string).
     * @default "ease"
     */
    easing?: string
    /**
     * Animation speed in ms for the top loader.
     * @default 200
     */
    speed?: number
    /**
     * Defines a shadow for the top loader.
     * @default "0 0 10px ${color},0 0 5px ${color}"
     *
     * @ you can disable it by setting it to `false`
     */
    shadow?: string | false
    /**
     * Defines a template for the top loader.
     * @default "<div class="bar" role="bar"><div class="peg"></div></div>
     * <div class="spinner" role="spinner"><div class="spinner-icon"></div></div>"
     */
    template?: string
    /**
     * Defines zIndex for the top loader.
     * @default 1600
     *
     */
    zIndex?: number
    /**
     * Function to determine if navigation should be ignored.
     * Receives the navigation object and returns true to ignore the navigation.
     * @default undefined
     */
    ignoreBeforeNavigate?: (navigation: BeforeNavigate) => boolean
    ignoreAfterNavigate?: (navigation: AfterNavigate) => boolean
  }

  let {
    color = '#29d',
    minimum = 0.08,
    trickleSpeed = 200,
    height = 3,
    trickle = true,
    showSpinner = true,
    easing = 'ease',
    speed = 200,
    shadow = `0 0 10px ${color},0 0 5px ${color}`,
    template = `<div class="bar" role="bar"><div class="peg"></div></div><div class="spinner" role="spinner"><div class="spinner-icon"></div></div>`,
    zIndex = 1600,
    ignoreBeforeNavigate,
    ignoreAfterNavigate
  }: Props = $props()

  const boxShadow =
    !shadow && shadow !== undefined
      ? ''
      : shadow
        ? `box-shadow:${shadow}`
        : `box-shadow:0 0 10px ${color},0 0 5px ${color}`

  let styleElement: HTMLStyleElement = $state(undefined!)

  let styles = $derived(
    `#nprogress{pointer-events:none}#nprogress .bar{background:${color};position:fixed;z-index:${zIndex};top:0;left:0;width:100%;height:${height}px}#nprogress .peg{display:block;position:absolute;right:0;width:100px;height:100%;${boxShadow};opacity:1;-webkit-transform:rotate(3deg) translate(0px,-4px);-ms-transform:rotate(3deg) translate(0px,-4px);transform:rotate(3deg) translate(0px,-4px)}#nprogress .spinner{display:block;position:fixed;z-index:${zIndex};top:15px;right:15px}#nprogress .spinner-icon{width:18px;height:18px;box-sizing:border-box;border:2px solid transparent;border-top-color:${color};border-left-color:${color};border-radius:50%;-webkit-animation:nprogress-spinner 400ms linear infinite;animation:nprogress-spinner 400ms linear infinite}.nprogress-custom-parent{overflow:hidden;position:relative}.nprogress-custom-parent #nprogress .bar,.nprogress-custom-parent #nprogress .spinner{position:absolute}@-webkit-keyframes nprogress-spinner{0%{-webkit-transform:rotate(0deg)}100%{-webkit-transform:rotate(360deg)}}@keyframes nprogress-spinner{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}`
  )

  onMount(() => {
    styleElement = document.createElement('style')
    document.head.appendChild(styleElement)

    nProgress.configure({
      showSpinner,
      trickle,
      trickleSpeed,
      minimum,
      easing,
      speed,
      template
    })

    return () => {
      document.head.removeChild(styleElement)
    }
  })

  run(() => {
    if (styleElement) {
      styleElement.textContent = styles
    }
  })

  beforeNavigate((navigation) => {
    // Check if navigation should be ignored via dependency injection
    if (ignoreBeforeNavigate?.(navigation)) {
      return
    }

    nProgress.start()
  })

  afterNavigate((navigation) => {
    // Check if navigation should be ignored via dependency injection
    if (ignoreAfterNavigate?.(navigation)) {
      return
    }

    nProgress.done()
  })
</script>

<svelte:head>
  <style>
		{@html styles}
  </style>
</svelte:head>
