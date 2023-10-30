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
Don't forget to first try out the [Docker demo container](../intro), which requires
no license key. The Docker demo already showcases many of Feldera's features.
This documentation is about setting up a distributed cloud deployment.
:::

## Setup

1. **[Load assets](assets):** using your license key, load the required assets
   into your cloud environment such that they are available for deployment.

2. **[Deployment](deployment):** start a Kubernetes cluster on which Feldera Cloud 
   is deployed.

## Usage

Once you have completed the setup, you can get started with using Feldera Cloud.
For example, consider the following resources:

* [Create your first pipeline](../tutorials/basics/part1)
* Browse our [source/input connectors](../connectors/sources)
  (such as [Kafka](../connectors/sources/kafka),
  [MySQL](../connectors/sources/debezium-mysql.md),
  and [HTTP](../connectors/sources/http))
* Browse our [sink/output connectors](../connectors/sinks)
  (such as [Kafka](../connectors/sinks/kafka),
  [Snowflake](../connectors/sinks/snowflake),
  and [HTTP](../connectors/sinks/http))

## Supported cloud providers

We currently support Amazon AWS. We are planning to expand our support
offering to other cloud providers such as GCP and Azure. Please reach out
if you are interested, we would be happy to hear more about how we can
facilitate your use case.
