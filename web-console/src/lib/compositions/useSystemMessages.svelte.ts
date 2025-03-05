import { useLocalStorage } from '$lib/compositions/localStore.svelte'
import { nubLast } from '$lib/functions/common/array'
import { P, match } from 'ts-pattern'
import { initSystemMessages, type SystemMessage } from '$lib/compositions/initSystemMessages'
import { untrack } from 'svelte'

type ShownSystemMessage = {
  messageId: string
  timestamp: number
}

let systemMessages: SystemMessage[] = $state([])

const browserOpenedTimestamp = window.performance.timeOrigin

export const useSystemMessages = () => {
  const shownMessages = useLocalStorage<ShownSystemMessage[]>('shownSystemMessages', [])
  const shownMessage = (id: string) => shownMessages.value.find((shown) => shown.messageId === id)
  $effect.pre(() => {
    untrack(() => {
      if (!systemMessages.length) {
        systemMessages = initSystemMessages
      }
    })
  })
  const displayedMessages = $derived.by(() => {
    // shownMessages.value
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
        .with({ milliseconds: P.select() }, (periodMs) => {
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
    dismiss(messageId: string) {
      const dismissed = systemMessages.find((message) => message.id === messageId)
      if (!dismissed) {
        return
      }
      shownMessages.value = nubLast(
        [{ messageId, timestamp: Date.now() }, ...shownMessages.value],
        (shown) => shown.messageId
      )
    }
  }
}
