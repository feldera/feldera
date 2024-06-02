# Feldera Cloud

Feldera Cloud is the enterprise offering of Feldera. It is designed such that
you can bring your own cloud, on which you deploy Feldera.
The documentation we provide here is publicly available, both to guide
our existing customers and to provide insight to interested parties.
A portion of the Feldera assets (such as container images) required to
actually perform the steps in this documentation are not publicly
available, and are only accessible with a license key.

Please contact us at `learnmore@feldera.com` if you are interested to learn
more or would like to try out Feldera Cloud. We'd be happy to get you started!

:::tip
Don't forget to first try out the [Docker demo container](/docs/docker), which requires
no license key. The Docker demo already showcases many of Feldera's features.
This documentation is about setting up a distributed cloud deployment.
:::

## Setup

1. **[Load assets](/docs/cloud/assets):** using your license key, load the required assets
   into your cloud environment such that they are available for deployment.

2. **[Deployment](/docs/cloud/deployment):** start a Kubernetes cluster on which Feldera Cloud
   is deployed.

## Usage

Once you have completed the setup, you can get started with using Feldera Cloud.
For example, consider the following resources:

* [Create your first pipeline](/docs/tutorials/basics/part1)
* Browse our [source/input connectors](/docs/connectors/sources)
  (such as [Kafka](/docs/connectors/sources/kafka),
  [MySQL](/docs/connectors/sources/debezium-mysql),
  and [HTTP](/docs/connectors/sources/http))
* Browse our [sink/output connectors](/docs/connectors/sinks)
  (such as [Kafka](/docs/connectors/sinks/kafka),
  [Snowflake](/docs/connectors/sinks/snowflake),
  and [HTTP](/docs/connectors/sinks/http))

## Supported cloud providers

We currently support Amazon AWS. We are planning to expand our support
offering to other cloud providers such as GCP and Azure. Please reach out
if you are interested, we would be happy to hear more about how we can
facilitate your use case.
