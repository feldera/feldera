import { untrack } from 'svelte'

// Types
type ReactiveState<T> = () => T
type Predicate<T> = (value: T) => boolean

interface GlobalWaiterOptions {
  timeout?: number
}

interface WaiterOptions<T> {
  predicate: Predicate<T>
  timeout?: number
  onSuccess?: (value: T) => void
  onError?: (error: Error) => void
  onTimeout?: () => void
  immediate?: boolean
}

interface WaiterState<T> {
  id: number
  predicate: Predicate<T>
  timeoutId: NodeJS.Timer | null
  isWaiting: boolean
  onSuccess: (value: T) => void
  onError: (error: Error) => void
  onTimeout: () => void
  resolve: (value: T) => void
  reject: (error: Error) => void
}

interface Waiter<T> {
  readonly id: number
  readonly isWaiting: boolean
  readonly error: Error | null
  waitFor: () => Promise<T>
  cancel: () => void
}

interface ReactiveWaiterHook<T> {
  readonly activeCount: number
  readonly isActive: boolean
  createWaiter: (options: WaiterOptions<T>) => Waiter<T>
  cancelAll: () => void
}

/**
 * A generic Svelte 5 hook for waiting on reactive state predicates
 * Creates waiters that can lazily wait for specific conditions on reactive state
 */
export function useReactiveWaiter<T>(
  reactiveState: ReactiveState<T>,
  options: GlobalWaiterOptions = {}
): ReactiveWaiterHook<T> {
  const { timeout: defaultTimeout } = options

  let activeWaiters = $state(new Map<number, WaiterState<T>>())
  let waiterCounter = 0

  // Single effect that monitors all active waiters
  $effect(() => {
    const currentValue = reactiveState()
    const waitingWaiters = Array.from(activeWaiters.values()).filter((waiter) => waiter.isWaiting)

    if (waitingWaiters.length === 0) return

    for (const waiter of waitingWaiters) {
      try {
        const predicateResult = waiter.predicate(currentValue)

        if (predicateResult === true) {
          // Predicate satisfied
          const { resolve, timeoutId, id, onSuccess } = waiter

          // Update state
          waiter.isWaiting = false

          // Cleanup timeout
          if (timeoutId) {
            clearTimeout(timeoutId)
            waiter.timeoutId = null
          }

          // Remove from active waiters
          activeWaiters.delete(id)

          // Resolve the promise and call success callback
          resolve(currentValue)
          onSuccess(currentValue)
        }
        // If predicate returns false, keep waiting (do nothing)
      } catch (error) {
        // Predicate threw an error
        const { reject, timeoutId, id, onError } = waiter

        // Update state
        waiter.isWaiting = false

        // Cleanup timeout
        if (timeoutId) {
          clearTimeout(timeoutId)
          waiter.timeoutId = null
        }

        // Remove from active waiters
        activeWaiters.delete(id)

        // Reject the promise and call error callback
        const predicateError = error instanceof Error ? error : new Error(String(error))
        reject(predicateError)
        onError(predicateError)
      }
    }
  })

  // Cleanup effect to dispose of all timers on unmount
  $effect(() => {
    return () => {
      // Cleanup all active waiters and their timers on unmount
      for (const [id, waiter] of activeWaiters) {
        if (waiter.timeoutId) {
          clearTimeout(waiter.timeoutId)
          waiter.timeoutId = null
        }
        waiter.reject(new Error('Component unmounted'))
      }
      activeWaiters.clear()
    }
  })

  const createWaiter = (waiterOptions: WaiterOptions<T>): Waiter<T> => {
    const waiterId = ++waiterCounter
    const {
      predicate,
      timeout,
      onSuccess = () => {},
      onError = () => {},
      onTimeout = () => {}
    } = waiterOptions
    
    const effectiveTimeout = timeout ?? defaultTimeout

    let waiterError = $state<Error | null>(null)

    const waitFor = (): Promise<T> => {
      return new Promise<T>((resolve, reject) => {
        if (waiterOptions.immediate) {
          // Check if predicate is already satisfied
          try {
            const currentValue = untrack(() => reactiveState())
            const predicateResult = predicate(currentValue)

            if (predicateResult === true) {
              // Already satisfied
              resolve(currentValue)
              onSuccess(currentValue)
              return
            }
            // If predicate returns false, continue to set up monitoring
          } catch (error) {
            // Predicate threw during initial check
            const predicateError = error instanceof Error ? error : new Error(String(error))
            onError(predicateError)
            reject(predicateError)
            return
          }
        }

        // Create waiter state
        const waiterState: WaiterState<T> = {
          id: waiterId,
          predicate,
          timeoutId: null,
          isWaiting: true,
          onSuccess,
          onError,
          onTimeout,
          resolve: (value: T) => {
            waiterError = null
            resolve(value)
          },
          reject: (error: Error) => {
            waiterError = error
            reject(error)
          }
        }

        // Set up timeout only if configured
        if (effectiveTimeout !== undefined) {
          waiterState.timeoutId = setTimeout(() => {
            const waiter = activeWaiters.get(waiterId)
            if (waiter && waiter.isWaiting) {
              activeWaiters.delete(waiterId)
              const error = new Error('Wait operation timed out')
              waiter.reject(error)
              waiter.onTimeout()
            }
          }, effectiveTimeout)
        }

        // Store the waiter
        activeWaiters.set(waiterId, waiterState)
      })
    }

    const cancel = (): void => {
      const waiter = activeWaiters.get(waiterId)
      if (waiter) {
        waiter.isWaiting = false
        if (waiter.timeoutId) {
          clearTimeout(waiter.timeoutId)
          waiter.timeoutId = null
        }
        activeWaiters.delete(waiterId)
        const error = new Error('Wait operation cancelled')
        waiter.reject(error)
      }

      waiterError = null
    }

    return {
      id: waiterId,
      get isWaiting() {
        const waiter = activeWaiters.get(waiterId)
        return waiter?.isWaiting ?? false
      },
      get error() {
        return waiterError
      },
      waitFor,
      cancel
    }
  }

  const cancelAll = (): void => {
    for (const [id, waiter] of activeWaiters) {
      waiter.isWaiting = false
      if (waiter.timeoutId) {
        clearTimeout(waiter.timeoutId)
        waiter.timeoutId = null
      }
      waiter.reject(new Error('All wait operations cancelled'))
    }
    activeWaiters.clear()
  }

  return {
    get activeCount() {
      return activeWaiters.size
    },
    get isActive() {
      return activeWaiters.size > 0
    },
    createWaiter,
    cancelAll
  }
}

// Example usage:
/*
<script lang="ts">
import { useReactiveWaiter } from './hooks/useReactiveWaiter.js';

// Example 1: Generic reactive waiter
type AppState = 'loading' | 'ready' | 'error' | 'updating' | 'updated';
let appState = $state<AppState>('loading');

const stateWaiter = useReactiveWaiter(
  () => appState,
  {
    timeout: 5000
  }
);

// Create waiters with specific predicates and callbacks
const readyWaiter = stateWaiter.createWaiter({
  predicate: state => {
    console.log('Checking if ready:', state);
    if (state === 'error') {
      throw new Error('App is in error state');
    }
    return state === 'ready';
  },
  onSuccess: (state) => console.log('App is ready!', state),
  onError: (error) => console.error('Ready wait failed:', error.message),
  onTimeout: () => console.log('Ready wait timed out')
});

const updateWaiter = stateWaiter.createWaiter({
  predicate: state => {
    console.log('Checking if updated:', state);
    // This will keep waiting while state is 'updating'
    // and resolve when state becomes 'updated'
    return state === 'updated';
  },
  timeout: 10000, // Custom timeout for this waiter
  onSuccess: (state) => console.log('Update completed!', state),
  onError: (error) => console.error('Update wait failed:', error)
});

// Wait for app to be ready
const waitForReady = async () => {
  try {
    const state = await readyWaiter.waitFor();
    console.log('Ready waiter resolved with state:', state);
  } catch (error) {
    console.error('Failed to wait for ready state:', error);
  }
};

// Wait for updates to complete
const waitForUpdate = async () => {
  try {
    const state = await updateWaiter.waitFor();
    console.log('Update waiter resolved with state:', state);
  } catch (error) {
    console.error('Update wait failed:', error);
  }
};

// Example 2: Multiple calls create independent promises
const handleMultipleWaits = async () => {
  // Create multiple waiters with different configurations
  const waiter1 = stateWaiter.createWaiter({
    predicate: state => {
      if (state === 'error') throw new Error('State is error');
      return state === 'ready';
    },
    onSuccess: () => console.log('Waiter 1 succeeded'),
    onError: (error) => console.log('Waiter 1 failed:', error.message),
    timeout: 3000
  });

  const waiter2 = stateWaiter.createWaiter({
    predicate: state => {
      console.log('Waiter 2 checking state:', state);
      return state === 'ready'; // Returns false for other states, keeps waiting
    },
    onSuccess: () => console.log('Waiter 2 succeeded'),
    timeout: 5000
  });

  const waiter3 = stateWaiter.createWaiter({
    predicate: state => {
      // This predicate will throw if state contains 'error'
      if (state.includes('error')) {
        throw new Error(`Invalid state: ${state}`);
      }
      return state === 'ready';
    },
    onSuccess: () => console.log('Waiter 3 succeeded'),
    onError: (error) => console.log('Waiter 3 failed:', error.message)
  });

  // Each creates independent promises
  const promise1 = waiter1.waitFor();
  const promise2 = waiter2.waitFor();
  const promise3 = waiter3.waitFor();

  // All resolve independently when the condition is met
  Promise.all([promise1, promise2, promise3]).then(() => {
    console.log('All waits completed!');
  });
};

// Simulate state changes
setTimeout(() => { appState = 'ready'; }, 2000);
setTimeout(() => { appState = 'updating'; }, 4000);
setTimeout(() => { appState = 'updated'; }, 6000);
</script>

<button onclick={waitForReady}>Wait for Ready</button>
<button onclick={waitForUpdate}>Wait for Update</button>
<button onclick={handleApiCall}>API Call with Status Wait</button>
<button onclick={handleMultipleWaits}>Multiple Independent Waits</button>

<p>Current state: {appState}</p>
<p>Active waiters: {stateWaiter.activeCount}</p>
*/
