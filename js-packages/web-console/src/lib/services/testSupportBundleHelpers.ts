/**
 * Shared test helpers for support bundle tests.
 */

/**
 * A fake `FileSystemFileHandle`, because a test cannot create a real one.
 *
 * IndexedDB saves only an object's own data fields. It rejects an object that holds a
 * function in such a field. So the methods here go on the prototype, and the object
 * itself holds only `name` and `kind`. A fake handle read back from IndexedDB then has
 * `name` and `kind` but no methods. A real handle keeps its methods.
 *
 * It has no permission methods, like a handle read back from IndexedDB, or any handle
 * in a browser other than Chromium.
 */
export const fakeHandle = (name: string, contents = 'bundle contents') =>
  Object.create(
    {
      getFile: async () => new File([contents], name),
      isSameEntry: async (other: { name: string }) => other.name === name
    },
    {
      name: { value: name, enumerable: true },
      kind: { value: 'file', enumerable: true }
    }
  ) as FileSystemFileHandle
