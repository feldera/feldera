import { EditorSchema, KafkaInputSchema, KafkaOutputSchema } from '$lib/components/connectors/dialogs'
import { DebeziumInputSchema } from '$lib/components/connectors/dialogs/DebeziumInputConnector'
import { SnowflakeOutputSchema } from '$lib/components/connectors/dialogs/SnowflakeOutputConnector'
import { assertUnion } from '$lib/functions/common/array'
import { parseAuthParams } from '$lib/functions/kafka/authParamsSchema'
import { fromKafkaConfig } from '$lib/functions/kafka/librdkafkaOptions'
import { ConnectorDescr, TransportConfig } from '$lib/services/manager'
import { ConnectorType, Direction } from '$lib/types/connectors'
import ImageBoilingFlask from '$public/icons/generic/boiling-flask.svg'
import ImageHttpGet from '$public/images/generic/http-get.svg'
import S3Logo from '$public/images/vendors/amazon-s3-logo.svg'
import DebeziumLogo from '$public/images/vendors/debezium-logo-color.svg'
import KafkaLogo from '$public/images/vendors/kafka-logo-black.svg'
import SnowflakeLogo from '$public/images/vendors/snowflake-logo.svg'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'
import iconBoilingFlask from '~icons/generic/boiling-flask'
import iconHttpGet from '~icons/tabler/http-get'
import iconS3 from '~icons/vendors/amazon-s3-icon'
import iconKafka from '~icons/vendors/apache-kafka-icon'
import iconDebezium from '~icons/vendors/debezium-icon-color'
import iconSnowflake from '~icons/vendors/snowflake-icon'

import { SVGImport } from '../types/imports'

// Determine the type of a connector from its config entries.
export const connectorDescrToType = (config: ConnectorDescr['config']): ConnectorType => {
  return match(config)
    .with(
      { transport: { name: TransportConfig.name.KAFKA_INPUT }, format: { config: { update_format: 'debezium' } } },
      () => {
        return ConnectorType.DEBEZIUM_IN
      }
    )
    .with(
      { transport: { name: TransportConfig.name.KAFKA_OUTPUT }, format: { config: { update_format: 'snowflake' } } },
      () => {
        return ConnectorType.SNOWFLAKE_OUT
      }
    )
    .with({ transport: { name: TransportConfig.name.KAFKA_INPUT } }, () => {
      return ConnectorType.KAFKA_IN
    })
    .with({ transport: { name: TransportConfig.name.KAFKA_OUTPUT } }, () => {
      return ConnectorType.KAFKA_OUT
    })
    .with({ transport: { name: TransportConfig.name.URL_INPUT } }, () => {
      return ConnectorType.URL_IN
    })
    .otherwise(() => {
      return ConnectorType.UNKNOWN
    })
}

export const parseConnectorDescrWith =
  <Config>(parseConfig: (config: ConnectorDescr['config']) => Config) =>
  (connector: ConnectorDescr) => {
    return {
      name: connector.name,
      description: connector.description,
      ...parseConfig(connector.config)
    }
  }

/**
 * Given an existing ConnectorDescr return the KafkaInputSchema
 * if connector is of type KAFKA_IN.
 */
export const parseKafkaInputSchemaConfig = (config: ConnectorDescr['config']) => {
  invariant(config.transport.name === TransportConfig.name.KAFKA_INPUT)

  const authConfig = parseAuthParams(config.transport.config)

  return {
    transport: {
      ...fromKafkaConfig(config.transport.config),
      ...authConfig
    } as KafkaInputSchema['transport'],
    format: {
      format_name: assertUnion(['json', 'csv'] as const, config.format.name),
      update_format: config.format.config?.update_format || 'raw',
      json_array: config.format.config?.array || false
    }
  }
}

export const parseKafkaInputSchema = parseConnectorDescrWith(parseKafkaInputSchemaConfig)

// Given an existing ConnectorDescr return the KafkaOutputSchema
// if connector is of type KAFKA_OUT.
export const parseKafkaOutputSchemaConfig = (config: ConnectorDescr['config']) => {
  invariant(config.transport.name === TransportConfig.name.KAFKA_OUTPUT)
  const authConfig = parseAuthParams(config.transport.config)

  return {
    transport: {
      ...fromKafkaConfig(config.transport.config),
      ...authConfig
    } as KafkaOutputSchema['transport'],
    format: {
      format_name: assertUnion(['json', 'csv'] as const, config.format.name),
      json_array: config.format.config?.array || false
    }
  }
}

export const parseKafkaOutputSchema = parseConnectorDescrWith(parseKafkaOutputSchemaConfig)

// Given an existing ConnectorDescr return the DebeziumInputSchema
// if connector is of type DEBEZIUM_IN.
export const parseDebeziumInputSchemaConfig = (config: ConnectorDescr['config']) => {
  invariant(config.transport.name === TransportConfig.name.KAFKA_INPUT)

  const authConfig = parseAuthParams(config.transport.config)
  return {
    transport: {
      ...fromKafkaConfig(config.transport.config),
      ...authConfig
    } as DebeziumInputSchema['transport'],
    format: {
      format_name: assertUnion(['json'] as const, config.format.name),
      update_format: assertUnion(['debezium'] as const, config.format!.config!.update_format),
      json_flavor: assertUnion(['debezium_mysql'] as const, config.format!.config!.json_flavor)
    }
  }
}

export const parseDebeziumInputSchema = parseConnectorDescrWith(parseDebeziumInputSchemaConfig)

export const parseSnowflakeOutputSchemaConfig = (config: ConnectorDescr['config']) => {
  invariant(config.transport.name === TransportConfig.name.KAFKA_OUTPUT)

  const authConfig = parseAuthParams(config.transport.config)

  return {
    transport: {
      ...fromKafkaConfig(config.transport.config),
      ...authConfig
    } as SnowflakeOutputSchema['transport'],
    format: {
      format_name: assertUnion(['json', 'avro'] as const, config.format.name),
      update_format: assertUnion(['snowflake'] as const, config.format!.config!.update_format)
    }
  }
}

export const parseSnowflakeOutputSchema = parseConnectorDescrWith(parseSnowflakeOutputSchemaConfig)

// Given an existing ConnectorDescr return the CsvFileSchema
// if connector is of type FILE.
export const parseUrlSchemaConfig = (config: ConnectorDescr['config']) => {
  invariant(config.transport.name === TransportConfig.name.URL_INPUT)

  return {
    transport: {
      url: config.transport.config.path
    },
    format: {
      format_name: assertUnion(['json', 'csv'] as const, config.format.name),
      update_format: config.format.config?.update_format || 'raw',
      json_array: config.format.config?.array || false
    }
  }
}

export const parseUrlSchema = parseConnectorDescrWith(parseUrlSchemaConfig)

// Given an existing ConnectorDescr return EditorSchema for it.
export const parseEditorSchema = (connector: ConnectorDescr): EditorSchema => {
  invariant(connector.config)
  return {
    name: connector.name,
    description: connector.description,
    transport: connector.config.transport,
    format: connector.config.format
  }
}

// Given a ConnectorType determine for what it can be used, inputs, outputs or
// both.
export const connectorTypeToDirection = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return Direction.OUTPUT
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return Direction.OUTPUT
    })
    .with(ConnectorType.S3_IN, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.URL_IN, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.UNKNOWN, () => {
      return Direction.INPUT_OUTPUT
    })
    .exhaustive()

/// Given a connector type return to which name in the config it corresponds to.
export const connectorTransportName = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return 'kafka_input'
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return 'kafka_output'
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return 'kafka_input'
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return 'kafka_output'
    })
    .with(ConnectorType.S3_IN, () => {
      return 's3_input'
    })
    .with(ConnectorType.URL_IN, () => {
      return 'url_input'
    })
    .with(ConnectorType.UNKNOWN, () => {
      return ''
    })
    .exhaustive()

// Return the title of a connector (for display in components).
export const connectorTypeToTitle = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return 'Kafka Input'
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return 'Kafka Output'
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return 'Debezium Input'
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return 'Snowflake Output'
    })
    .with(ConnectorType.S3_IN, () => {
      return 'S3 Compatible Input'
    })
    .with(ConnectorType.URL_IN, () => {
      return 'HTTP URL'
    })
    .with(ConnectorType.UNKNOWN, () => {
      return 'Connector'
    })
    .exhaustive()

// Return the icon of a connector (for display in components).
export const connectorTypeToLogo = (status: ConnectorType): SVGImport =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return KafkaLogo
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return KafkaLogo
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return DebeziumLogo
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return SnowflakeLogo
    })
    .with(ConnectorType.S3_IN, () => {
      return S3Logo
    })
    .with(ConnectorType.URL_IN, () => {
      return ImageHttpGet
    })
    .with(ConnectorType.UNKNOWN, () => {
      return ImageBoilingFlask
    })
    .exhaustive()

// Return the icon of a connector (for display in components).
export const connectorTypeToIcon = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return iconKafka
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return iconKafka
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return iconDebezium
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return iconSnowflake
    })
    .with(ConnectorType.S3_IN, () => {
      return iconS3
    })
    .with(ConnectorType.URL_IN, () => {
      return iconHttpGet
    })
    .with(ConnectorType.UNKNOWN, () => {
      return iconBoilingFlask
    })
    .exhaustive()

// Return name and color (for display of the table' chip) of a connector.
export const getStatusObj = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return { title: 'Kafka In', color: 'secondary' as const }
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return { title: 'Kafka Out', color: 'secondary' as const }
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return { title: 'Debezium In', color: 'secondary' as const }
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return { title: 'Snowflake Out', color: 'secondary' as const }
    })
    .with(ConnectorType.S3_IN, () => {
      return { title: 'S3 In', color: 'secondary' as const }
    })
    .with(ConnectorType.URL_IN, () => {
      return { title: 'HTTP GET', color: 'secondary' as const }
    })
    .with(ConnectorType.UNKNOWN, () => {
      return { title: 'Editor', color: 'secondary' as const }
    })
    .exhaustive()
