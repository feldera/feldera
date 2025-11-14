import { useLocalStorage } from '$lib/compositions/localStore.svelte'
import { findSplice, nubLast, singleton } from '$lib/functions/common/array'
import { P, match } from 'ts-pattern'
import { initSystemMessages, type SystemMessage } from '$lib/compositions/initSystemMessages'
import { untrack } from 'svelte'

type ShownSystemMessage = {
  id: string
  timestamp: number
}

let systemMessages: SystemMessage[] = $state([])

const browserOpenedTimestamp = window.performance.timeOrigin

export const useSystemMessages = () => {
  const shownMessages = useLocalStorage<ShownSystemMessage[]>('shownSystemMessages', []) // Contains info about dismissed messages (when they were last shown)
  const shownMessage = (id: string) => shownMessages.value.find((message) => message.id === id)
  $effect.pre(() => {
    untrack(() => {
      if (!systemMessages.length) {
        systemMessages = initSystemMessages
      }
    })
  })
  const displayedMessages = $derived.by(() => {
    return systemMessages.filter((message) => {
      return match(message.dismissable)
        .with('never', () => true)
        .with('once', () => !shownMessage(message.id))
        .with('session', () => {
          const shownTimestamp = shownMessage(message.id)?.timestamp
          // Display the message on every browser session or once a day
          const startOfTheDayTimestamp = ((now) => now - (now % 1000) * 3600 * 24)(Date.now())
          return (
            !shownTimestamp ||
            shownTimestamp < Math.max(browserOpenedTimestamp, startOfTheDayTimestamp)
          )
        })
        .with({ forMs: P.select() }, (periodMs) => {
          const shownTimestamp = shownMessage(message.id)?.timestamp
          return !shownTimestamp || shownTimestamp <= Date.now() - periodMs
        })
        .exhaustive()
    })
  })

  return {
    get displayedMessages() {
      return displayedMessages
    },
    push(message: SystemMessage) {
      systemMessages.push(message)
    },
    dismiss(id: string) {
      const dismissed = systemMessages.find((message) => message.id === id)
      if (!dismissed) {
        return
      }
      if (dismissed.dismissable === 'never') {
        throw new Error('The message cannot be dismissed')
      }
      shownMessages.value = nubLast(
        [{ id, timestamp: Date.now() }, ...shownMessages.value],
        (shown) => shown.id
      )
    },
    /**
     * If the element to replace is found - remove it or replace with the newMessage
     * If the element to replace is not found - push the newMessage
     */
    replace(id: string | RegExp, newMessage: SystemMessage | null) {
      if (typeof id === 'string' && id === newMessage?.id) {
        return
      }
      const matchesId = (message: { id: string }) =>
        typeof id === 'string' ? message.id === id : id.test(message.id)
      if (!findSplice(systemMessages, matchesId, ...singleton(newMessage))) {
        if (newMessage) {
          systemMessages.push(newMessage)
        }
        return
      }
      {
        if (!findSplice(shownMessages.value, matchesId)) {
          return
        }
        // Trigger reactive update
        shownMessages.value = shownMessages.value
      }
    }
  }
}
