# webui-tester

Is an utility crate to run integration tests against the web-console.

## Setup

You need a webdriver (chrome works best, the firefox driver needs to be
restarted every time a test doesn't exit cleanly) or selenium instance to run
these tests. Check the [thirtyfour
README](https://github.com/stevepryde/thirtyfour#running-against-selenium) for
instructions.

## Configuration

The following environment variables can be set to control test execution:

- WEBUI_TESTER_HOST: The host where the webui is running (defaults to localhost)
- WEBUI_TESTER_PORT: The port where the webui is running (defaults to 8080)
- WEBUI_TESTER_DRIVER_HOST: The host where the webdriver is running (defaults to localhost)
- WEBUI_TESTER_DRIVER_PORT: The port where the webdriver is running (defaults to 9515)

## Example execution

```bash
# make sure you have started e.g., the chromedriver
WEBUI_TESTER_HOST=myhost cargo test
```
