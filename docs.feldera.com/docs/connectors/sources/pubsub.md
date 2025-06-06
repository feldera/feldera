# Google Pub/Sub input connector

:::note
This page describes configuration options specific to the Google Pub/Sub connector.
See [top-level connector documentation](/connectors/) for general information
about configuring input and output connectors.
:::

The Google Pub/Sub connector is used to consume a stream of changes to a SQL table from a Pub/Sub subscription.

The Google Pub/Sub input connector does not yet support [fault
tolerance](/pipelines/fault-tolerance).

## Configuration options

This connector supports four groups of configuration options:

1. [Authentication options](#authentication) control Google Cloud authentication
2. [Subscription options](#subscription) specify an existing Pub/Sub subscription from which
   the connector will pull messages
3. [Connectivity options](#connectivity) configure gRPC connection to the Pub/Sub service
4. [Emulator options](#emulator) are used to connect to a Pub/Sub emulator instead of the
   real Google Pub/Sub service

The only required option that must be present in any valid Pub/Sub connector configuration is
`subscription`.

### Authentication

* `credentials` - The content of a Google Cloud credentials JSON file.

  In order to access a Pub/Sub topic, the connector must authenticate with Google Cloud.
  When Feldera runs in an environment with
  [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials)
  configured, the connector will pickup the credentials automatically from the
  `GOOGLE_APPLICATION_CREDENTIALS` environment variable or from the
  `$HOME/.config/gcloud/application_default_credentials.json` file.

  When running Feldera in an environment where ADC are not configured,
  e.g., a Docker container, use the `credentials` option to ship Google Cloud
  credentials from another environment.  For example, if you use the
  [`gcloud auth application-default login`](https://cloud.google.com/pubsub/docs/authentication#client-libs)
  command for authentication in your local development environment, ADC are stored in the
  `$HOME/.config/gcloud/application_default_credentials.json` file.

  Note that authentication is not required when running against a Pub/Sub [emulator](#emulator).

  See [example](#example) below for more details on configuring `credentials`.

* `project_id` - Google Cloud project_id.
   When not specified, the connector will use the project id associated
   with the authenticated account.

### Subscription

* `subscription` - Pub/Sub subscription name.  The subscription must exist.

* `snapshot` -  Reset subscription's backlog to a given snapshot on startup,
  using the Pub/Sub `Seek` API.
  This option is mutually exclusive with the `timestamp` option.

* `timestamp` - Reset subscription's backlog to a given timestamp on startup,
  using the Pub/Sub `Seek` API. The value of this field is an
  ISO 8601-encoded UTC time, e.g., "2024-08-17T16:39:57-08:00".
  This option is mutually exclusive with the `snapshot` option.

### Connectivity

These options configure gRPC connection to the Pub/Sub service.

* `endpoint` - Overrides the default service endpoint 'pubsub.googleapis.com'.

* `pool_size` - gRPC channel pool size.

* `timeout_seconds` - gRPC request timeout.

* `connect_timeout_seconds` - gRPC connection timeout.

### Emulator

* `emulator` - Pub/Sub emulator URL in the 'host:port' format. Use this option
  to connect to a Pub/Sub [emulator](https://cloud.google.com/pubsub/docs/emulator)
  instead of the production service.

## Example

In this section, we provide a step-by-step guide to configuring and using a Feldera Pub/Sub connector.
Please ensure you have a Google Cloud account before proceeding.

1. Create Application Default Credentials in your local development environment.

   Install `gcloud`, the [Google Cloud CLI](https://cloud.google.com/cli?hl=en)

   ```bash
   # Opens a Web browser for authentication.
   gcloud init
   gcloud auth application-default login
   ```

   The second command will store [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials)
   in the `$HOME/.config/gcloud/application_default_credentials.json` file.

2. Use `gcloud` CLI to create a test topic and a subscription.

   ```bash
   gcloud pubsub topics create test-topic
   gcloud pubsub subscriptions create test-subscription --retain-acked-messages --topic test-topic
   ```

3. Publish some messages to the topic

   ```bash
   for i in {1..10}; do gcloud pubsub topics publish test-topic --message "{\"id\": $i}"; done
   ```

4. Ingest messages in Feldera.

  Copy the following code to the Feldera WebConsole, replacing the `credentials` field with
  **escaped content of `$HOME/.config/gcloud/application_default_credentials.json`**.

  ```sql
  CREATE TABLE pub_sub_input (
    id int
  ) WITH (
      'connectors' = '[
      {
        "transport": {
            "name": "pub_sub_input",
            "config": {
                "subscription": "test-subscription",
                "timestamp": "2024-08-17T00:00:00-00:00",
                "credentials": "{\"account\": \"\",\"client_id\": \"<CLIENT_ID>\",\"client_secret\": \"<CLIENT_SECRET>\", \"quota_project_id\": \"feldera-test\", \"refresh_token\": \"<REFRESH_TOKEN>\",  \"type\": \"authorized_user\",  \"universe_domain\": \"googleapis.com\"}",
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
  );
  ```

  Select the `pub_sub_input` table in the "Changes stream" tab and run the pipeline.  You should see the ten messages
  you published in Step 3 in the change stream of the pipeline.

## Additional resources

For more information, see:

* [Top-level connector documentation](/connectors/)
* [Supported data formats](/formats)
