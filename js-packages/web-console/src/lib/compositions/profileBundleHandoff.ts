/**
 * Cross-tab handoff for uploaded support bundle ArrayBuffers.
 *
 * Hybrid transport:
 *   - Control plane (READY / ACK): BroadcastChannel keyed by a UUID in the URL,
 *     so the handshake survives any OIDC redirect chain the new tab may go
 *     through before the profile-viewer page mounts.
 *   - Data plane (the bundle bytes): window-to-window postMessage with the
 *     ArrayBuffer in the Transferable list — zero-copy. After transfer the
 *     source's reference is detached, so peak memory drops from 2× the bundle
 *     down to 1× the bundle. This matters: bundles can be hundreds of MB.
 *
 * Two-phase protocol:
 *   Phase 1 — READY wait: untimed; source polls child.closed every ~1 s so the
 *     child can take as long as needed (OIDC silent renew, interactive login,
 *     slow load). Aborts cleanly within ~1 s if the user closes the tab.
 *   Phase 2 — transfer + ACK: tight hard timeout. With Transferable postMessage
 *     the round-trip is just event-loop scheduling (~four microtasks), no byte
 *     copy regardless of bundle size — anything beyond ~1 s is a real fault.
 */

const READY_MSG = 'profile-bundle-ready'
const BUNDLE_MSG = 'profile-bundle-data'
const ACK_MSG = 'profile-bundle-ack'
const CHANNEL_PREFIX = 'profile-bundle:'
// Source's wait for the child's ACK after the bundle has been transferred.
// The Transferable postMessage + ACK round-trip is sub-millisecond on the
// happy path; 1 s leaves ~1000× margin while surfacing a stuck child fast.
const ACK_TIMEOUT_MS = 1_000
const CLOSED_POLL_MS = 1_000
// Child's wait for the bundle to arrive after posting READY. Fires when no
// source is listening — e.g. the viewer tab was reloaded after the original
// handoff completed, or the URL was opened directly. Sized just above
// ACK_TIMEOUT_MS so that if both sides race, the source's timeout reports
// the failure first (with a more specific error).
const RECEIVE_TIMEOUT_MS = 2_000

export class ProfileBundleUnavailableError extends Error {
  constructor() {
    super(
      'No upload bundle available on this channel. The originating tab may have ' +
        'been closed — re-upload the bundle to view it here.'
    )
    this.name = 'ProfileBundleUnavailableError'
  }
}

export function openRemoteBundleTab(pipelineName: string, collect: boolean) {
  const url = `/profile-viewer?pipelineName=${encodeURIComponent(pipelineName)}&source=remote&collect=${collect ? '1' : '0'}`
  window.open(url, '_blank')
}

export type UploadBundleHandoff = {
  /** Transfer the bundle once its bytes are available. */
  send: (arrayBuffer: ArrayBuffer) => Promise<void>
  /** Abort the handoff if the caller never reaches `send` (e.g. file read fails). */
  cancel: () => void
}

/**
 * Opens a profile-viewer tab and prepares the cross-tab channel.
 *
 * MUST be called synchronously from a user-gesture handler — `window.open` is
 * blocked as a popup if any `await` separates it from the gesture. Callers do
 * any async work (e.g. `file.arrayBuffer()`) AFTER this call returns, then
 * invoke `send` with the bundle bytes.
 *
 * The READY listener is attached synchronously here so a fast child that
 * posts READY before `send` is awaited isn't missed.
 *
 * The buffer passed to `send` is detached after it resolves — callers must
 * not read it afterwards.
 */
export function openUploadBundleTab(): UploadBundleHandoff {
  const channelId = crypto.randomUUID()
  // No pipelineName in the URL: the viewer reads it from the uploaded bundle's
  // pipeline_config.json once the bytes arrive over the channel.
  const url = `/profile-viewer?source=upload&channel=${channelId}`

  const child = window.open(url, '_blank')
  if (!child) {
    throw new Error('Browser blocked the popup. Allow popups for this site and try again.')
  }

  const channel = new BroadcastChannel(`${CHANNEL_PREFIX}${channelId}`)
  const readyPromise = waitForReady(channel, child)
  // Prevent an unhandledrejection if the caller cancels before awaiting send.
  readyPromise.catch(() => {})

  let closed = false
  const close = () => {
    if (!closed) {
      closed = true
      channel.close()
    }
  }

  return {
    async send(arrayBuffer: ArrayBuffer) {
      try {
        await readyPromise
        await transferAndAck(channel, child, channelId, arrayBuffer)
      } finally {
        close()
      }
    },
    cancel: close
  }
}

/** Phase 1: block until child posts READY; reject if the tab is closed first. */
function waitForReady(channel: BroadcastChannel, child: Window): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let poll: ReturnType<typeof setInterval> | undefined

    channel.onmessage = (event: MessageEvent) => {
      if (event.data?.type !== READY_MSG) {
        return
      }
      clearInterval(poll)
      channel.onmessage = null
      resolve()
    }

    poll = setInterval(() => {
      if (child.closed) {
        clearInterval(poll)
        reject(
          new Error('The profile viewer tab was closed before the bundle could be transferred.')
        )
      }
    }, CLOSED_POLL_MS)
  })
}

/**
 * Phase 2: zero-copy transfer the bundle to the child via Window postMessage with
 * the ArrayBuffer in the Transferable list, then wait for the child's ACK over
 * the BroadcastChannel. After the postMessage call the source's `arrayBuffer`
 * reference is detached (byteLength === 0).
 */
function transferAndAck(
  channel: BroadcastChannel,
  child: Window,
  channelId: string,
  arrayBuffer: ArrayBuffer
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      channel.onmessage = null
      reject(
        new Error(
          `Bundle transfer timed out — the viewer tab did not acknowledge receipt within ${ACK_TIMEOUT_MS} ms.`
        )
      )
    }, ACK_TIMEOUT_MS)

    channel.onmessage = (event: MessageEvent) => {
      if (event.data?.type !== ACK_MSG) {
        return
      }
      clearTimeout(timer)
      channel.onmessage = null
      resolve()
    }

    try {
      // Zero-copy: [arrayBuffer] in the transfer list moves ownership to the
      // child. The buffer becomes detached on this side immediately.
      // targetOrigin is fixed to our own origin (NOT '*') so the message cannot
      // be picked up by an unrelated origin if the child has navigated.
      child.postMessage(
        { type: BUNDLE_MSG, channelId, buffer: arrayBuffer },
        window.location.origin,
        [arrayBuffer]
      )
    } catch (e) {
      clearTimeout(timer)
      channel.onmessage = null
      reject(e instanceof Error ? e : new Error(String(e)))
    }
  })
}

/**
 * Called in the new viewer tab once it has mounted.
 * Installs a window-message listener for the bundle, signals readiness on the
 * BroadcastChannel, ACKs on the same channel once the bundle is received.
 * @param channelId UUID read from the `channel` URL query param.
 */
export async function receiveUploadedBundle(channelId: string): Promise<ArrayBuffer> {
  const channel = new BroadcastChannel(`${CHANNEL_PREFIX}${channelId}`)

  return new Promise<ArrayBuffer>((resolve, reject) => {
    const onMessage = (event: MessageEvent) => {
      // Same-origin only — even though postMessage with a fixed targetOrigin
      // restricts who can RECEIVE, this guard makes sure we don't ACCEPT a
      // message posted from anywhere else.
      if (event.origin !== window.location.origin) {
        return
      }
      if (event.data?.type !== BUNDLE_MSG) {
        return
      }
      // The channelId from the URL acts as a per-handoff shared secret: any
      // same-origin code can postMessage to us, but only code that read this
      // tab's URL (the source that opened us) knows the channelId.
      if (event.data?.channelId !== channelId) {
        return
      }

      clearTimeout(timer)
      window.removeEventListener('message', onMessage)
      const buffer = event.data.buffer as ArrayBuffer
      // ACK before resolving so the source tab's ACK_TIMEOUT_MS window is not
      // consumed by our downstream processing.
      channel.postMessage({ type: ACK_MSG })
      channel.close()
      resolve(buffer)
    }

    const timer = setTimeout(() => {
      window.removeEventListener('message', onMessage)
      channel.close()
      reject(new ProfileBundleUnavailableError())
    }, RECEIVE_TIMEOUT_MS)

    // Install the data-plane listener BEFORE signalling READY, so a fast source
    // that posts the bundle in the same microtask cannot beat us to it.
    window.addEventListener('message', onMessage)
    channel.postMessage({ type: READY_MSG })
  })
}
