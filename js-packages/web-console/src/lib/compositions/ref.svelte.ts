export class Ref<T> {
  private state: T
  constructor(value: T) {
    this.state = $state(value)
  }

  get current() {
    return this.state
  }
  set current(value: T) {
    this.state = value
  }
}
