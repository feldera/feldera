# Web Console testing

## e2e testing with Playwright
Existing Playwright tests are executed during CI
and can be run manually within provided devcontainer environment.

If you want to use `Playwright codegen` to automatically create new tests from UI interactions,
install Playwright on your host system.

Keep in mind that codegen is not designed to produce production-ready code,
so you might need to edit it.

```bash
yarn playwright codegen http://localhost:3000/
```

Prefer using `data-testid` prop and `.getByTestId()` to locate the elements.
When that is inconvenient, consider `.getByRole()`.
Resort to visible text-based locators when above methods are inconvenient.
Avoid locating by HTML element names, CSS classes and `id` prop.
When it is impractical to decorate a concrete HTML element with `data-testid` prop -
decorate its wrapping element, and then seek from this wrapper by target role, HTML element name or label.
