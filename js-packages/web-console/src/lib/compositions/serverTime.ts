/**
 * A drop-in replacement for `Date` that reads the current time from the
 * server's clock rather than the client's.
 *
 * The pipeline manager reports its own wall-clock time (e.g. in the license
 * payload). The browser clock may be skewed relative to the server, so any UI
 * that compares server-provided timestamps against "now" must use the server's
 * notion of now. `ServerDate` measures the offset once via {@link ServerDate.sync}
 * and applies it to every `new ServerDate()` and {@link ServerDate.now}.
 *
 * Instances are ordinary `Date` objects, so a `ServerDate` can be used anywhere
 * a `Date` is expected. Constructing one with an explicit argument behaves
 * exactly like `Date`; only the zero-argument form (the current time) is
 * shifted onto the server clock.
 */
export class ServerDate extends Date {
  /** Server clock minus client clock, in milliseconds. */
  private static offsetMs = 0

  /**
   * Records the client/server clock offset from a known server timestamp.
   * @param serverTime - The server's current time (ISO string, epoch ms, or `Date`).
   */
  static sync(serverTime: string | number | Date) {
    ServerDate.offsetMs = new Date(serverTime).getTime() - Date.now()
  }

  /** Current server time, in milliseconds since the Unix epoch. */
  static now(): number {
    return Date.now() + ServerDate.offsetMs
  }

  /**
   * @param value - As `Date`'s constructor; omit to use the current server time.
   */
  constructor(value?: number | string | Date) {
    super(value === undefined ? ServerDate.now() : value)
  }
}
