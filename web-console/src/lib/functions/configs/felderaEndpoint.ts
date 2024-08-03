export const felderaEndpoint =
  'window' in globalThis && window.location.origin
    ? // If we're running locally with `bun run dev`, we point to the
      // backend server running on port 8080
      // Otherwise the API and UI URL will be the same
      window.location.origin.replace(/:([45]173)$/, ':8080')
    : 'http://localhost:8080'
