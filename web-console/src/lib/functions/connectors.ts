import { DebeziumInputSchema } from '$lib/components/connectors/dialogs/DebeziumInputConnector'
import { EditorSchema } from '$lib/components/connectors/dialogs/GenericEditorConnector'
import { KafkaInputSchema } from '$lib/components/connectors/dialogs/KafkaInputConnector'
import { KafkaOutputSchema } from '$lib/components/connectors/dialogs/KafkaOutputConnector'
import { SnowflakeOutputSchema } from '$lib/components/connectors/dialogs/SnowflakeOutputConnector'
import { assertUnion } from '$lib/functions/common/array'
import { nonNull } from '$lib/functions/common/function'
import { parseAuthParams } from '$lib/functions/kafka/authParamsSchema'
import {
  fromKafkaConfig,
  LibrdkafkaOptionType,
  toKafkaConfig
} from '$lib/functions/kafka/librdkafkaOptions'
import { ConnectorDescr, OutputBufferConfig, TransportConfig } from '$lib/services/manager'
import { ConnectorType, Direction } from '$lib/types/connectors'
import { SVGImport } from '$lib/types/imports'
import IconGenericBoilingFlask from '$public/icons/generic/boiling-flask.svg'
import IconGenericHttpGet from '$public/icons/generic/http-get.svg'
import IconVendorsAmazonS3 from '$public/icons/vendors/amazon-s3.svg'
import IconVendorsApacheKafka from '$public/icons/vendors/apache-kafka.svg'
import IconVendorsDeltaLake from '$public/icons/vendors/databricks-delta-lake-icon.svg'
import IconVendorsDebezium from '$public/icons/vendors/debezium.svg'
import IconVendorsSnowflake from '$public/icons/vendors/snowflake.svg'
import ImageHttpGet from '$public/images/generic/http-get.svg'
import S3Logo from '$public/images/vendors/amazon-s3-logo.svg'
import DeltaLakeLogo from '$public/images/vendors/databricks-delta-lake-logo.svg'
import DebeziumLogo from '$public/images/vendors/debezium-logo-color.svg'
import KafkaLogo from '$public/images/vendors/kafka-logo-black.svg'
import SnowflakeLogo from '$public/images/vendors/snowflake-logo.svg'
import { BigNumber } from 'bignumber.js/bignumber.js'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'

// Determine the type of a connector from its config entries.
export const connectorDescrToType = (config: ConnectorDescr['config']): ConnectorType => {
  return match(config)
    .with(
      {
        transport: { name: TransportConfig.name.KAFKA_INPUT },
        format: { config: { update_format: 'debezium' } }
      },
      () => ConnectorType.DEBEZIUM_IN
    )
    .with(
      {
        transport: { name: TransportConfig.name.KAFKA_OUTPUT },
        format: { config: { update_format: 'snowflake' } }
      },
      () => ConnectorType.SNOWFLAKE_OUT
    )
    .with({ transport: { name: TransportConfig.name.KAFKA_INPUT } }, () => ConnectorType.KAFKA_IN)
    .with({ transport: { name: TransportConfig.name.KAFKA_OUTPUT } }, () => ConnectorType.KAFKA_OUT)
    .with({ transport: { name: TransportConfig.name.URL_INPUT } }, () => ConnectorType.URL_IN)
    .with(
      { transport: { name: TransportConfig.name.DELTA_TABLE_INPUT } },
      () => ConnectorType.DELTALAKE_IN
    )
    .with(
      { transport: { name: TransportConfig.name.DELTA_TABLE_OUTPUT } },
      () => ConnectorType.DELTALAKE_OUT
    )
    .with({ transport: { name: TransportConfig.name.S3_INPUT } }, () => ConnectorType.S3_IN)
    .with({ transport: { name: TransportConfig.name.FILE_INPUT } }, () => ConnectorType.UNKNOWN)
    .with({ transport: { name: TransportConfig.name.FILE_OUTPUT } }, () => ConnectorType.UNKNOWN)
    .exhaustive()
}

export const parseConnectorDescrWith =
  <Config>(parseConfig: (config: ConnectorDescr['config']) => Config) =>
  (connector: ConnectorDescr) => {
    return {
      name: connector.name,
      description: connector.description,
      enable_output_buffer: connector.config.enable_output_buffer,
      max_output_buffer_time_millis: ((n) => (nonNull(n) ? BigNumber(n) : n))(
        connector.config.max_output_buffer_time_millis
      ),
      max_output_buffer_size_records: ((n) => (nonNull(n) ? BigNumber(n) : n))(
        connector.config.max_output_buffer_size_records
      ),
      ...parseConfig(connector.config)
    }
  }

// Define what should happen when the form is submitted
export const prepareDataWith =
  <
    T extends {
      transport: Record<string, any>
      format?: Record<string, any>
    },
    R
  >(
    normalizeConfig: (config: T) => R
  ) =>
  (
    data: {
      name: string
      description?: string
    } & T &
      OutputBufferConfig
  ) => ({
    name: data.name,
    description: data.description,
    config: {
      ...normalizeConfig(data),
      enable_output_buffer: data.enable_output_buffer,
      max_output_buffer_time_millis: data.max_output_buffer_time_millis,
      max_output_buffer_size_records: data.max_output_buffer_size_records
    }
  })

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

export const normalizeKafkaInputConfig = (data: {
  transport: KafkaInputSchema['transport']
  format: Record<string, string | boolean>
}) => ({
  transport: {
    name: TransportConfig.name.KAFKA_INPUT,
    config: toKafkaConfig(data.transport)
  },
  format: {
    name: data.format.format_name,
    config: {
      ...(data.format.format_name === 'json'
        ? {
            update_format: data.format.update_format,
            array: data.format.json_array
          }
        : {})
    }
  }
})

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

export const normalizeKafkaOutputConfig = (data: {
  transport: Record<string, LibrdkafkaOptionType>
  format: Record<string, string | boolean>
}) => ({
  transport: {
    name: TransportConfig.name.KAFKA_OUTPUT,
    config: toKafkaConfig(data.transport)
  },
  format: {
    name: data.format.format_name,
    config: {
      ...(data.format.format_name === 'json'
        ? {
            array: data.format.json_array
          }
        : {})
    }
  }
})

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

export const normalizeDebeziumInputConfig = (data: {
  transport: DebeziumInputSchema['transport']
  format: Record<string, string | boolean>
}) => ({
  transport: {
    name: TransportConfig.name.KAFKA_INPUT,
    config: toKafkaConfig(data.transport)
  },
  format: {
    name: data.format.format_name,
    config: {
      ...(data.format.format_name === 'json'
        ? {
            update_format: data.format.update_format,
            json_flavor: data.format.json_flavor
          }
        : {})
    }
  }
})

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

export const normalizeSnowflakeOutputConfig = (data: {
  transport: Record<string, LibrdkafkaOptionType>
  format: Record<string, string | boolean>
}) => ({
  transport: {
    name: TransportConfig.name.KAFKA_OUTPUT,
    config: toKafkaConfig(data.transport)
  },
  format: {
    name: data.format.format_name,
    config: {
      update_format: data.format.update_format
    }
  }
})

export const parseDeltaLakeInputSchemaConfig = (config: ConnectorDescr['config']) => {
  invariant(config.transport.name === TransportConfig.name.DELTA_TABLE_INPUT)
  return {
    transport: {
      ...((config) => ({
        ...config,
        datetime: config.datetime ?? undefined,
        snapshot_filter: config.snapshot_filter ?? undefined,
        timestamp_column: config.timestamp_column ?? undefined,
        version: config.version ? BigNumber(config.version) : undefined
      }))(config.transport.config)
    }
  }
}

export const normalizeDeltaLakeInputConfig = (data: { transport: Record<string, any> }) => ({
  transport: {
    name: TransportConfig.name.DELTA_TABLE_INPUT,
    config: data.transport
  }
})

export const parseDeltaLakeOutputSchemaConfig = (config: ConnectorDescr['config']) => {
  invariant(config.transport.name === TransportConfig.name.DELTA_TABLE_OUTPUT)
  return {
    transport: {
      ...config.transport.config
    }
  }
}

export const normalizeDeltaLakeOutputConfig = (data: { transport: Record<string, any> }) => ({
  transport: {
    name: TransportConfig.name.DELTA_TABLE_OUTPUT,
    config: data.transport
  }
})

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
    .with(ConnectorType.DELTALAKE_IN, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.DELTALAKE_OUT, () => {
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

// Return the title of a connector (for display in components).
export const connectorTypeToTitle = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => ({
      full: 'Kafka Input',
      short: 'Kafka In'
    }))
    .with(ConnectorType.KAFKA_OUT, () => ({
      full: 'Kafka Output',
      short: 'Kafka Out'
    }))
    .with(ConnectorType.DEBEZIUM_IN, () => ({
      full: 'Debezium Input',
      short: 'Debezium In'
    }))
    .with(ConnectorType.SNOWFLAKE_OUT, () => ({
      full: 'Snowflake Output',
      short: 'Snowflake Out'
    }))
    .with(ConnectorType.DELTALAKE_IN, () => ({
      full: 'Delta Lake Input',
      short: 'DeltaLake In'
    }))
    .with(ConnectorType.DELTALAKE_OUT, () => ({
      full: 'Delta Lake Output',
      short: 'DeltaLake Out'
    }))
    .with(ConnectorType.S3_IN, () => ({
      full: 'S3 Compatible Input',
      short: 'S3 In'
    }))
    .with(ConnectorType.URL_IN, () => ({
      full: 'HTTP GET',
      short: 'HTTP GET'
    }))
    .with(ConnectorType.UNKNOWN, () => ({
      full: 'Generic Connector',
      short: 'Generic'
    }))
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
    .with(ConnectorType.DELTALAKE_IN, () => {
      return DeltaLakeLogo
    })
    .with(ConnectorType.DELTALAKE_OUT, () => {
      return DeltaLakeLogo
    })
    .with(ConnectorType.S3_IN, () => {
      return S3Logo
    })
    .with(ConnectorType.URL_IN, () => {
      return ImageHttpGet
    })
    .with(ConnectorType.UNKNOWN, () => {
      return IconGenericBoilingFlask
    })
    .exhaustive()

// Return the icon of a connector (for display in components).
export const connectorTypeToIcon = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return IconVendorsApacheKafka
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return IconVendorsApacheKafka
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return IconVendorsDebezium
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return IconVendorsSnowflake
    })
    .with(ConnectorType.DELTALAKE_IN, () => {
      return IconVendorsDeltaLake
    })
    .with(ConnectorType.DELTALAKE_OUT, () => {
      return IconVendorsDeltaLake
    })
    .with(ConnectorType.S3_IN, () => {
      return IconVendorsAmazonS3
    })
    .with(ConnectorType.URL_IN, () => {
      return IconGenericHttpGet
    })
    .with(ConnectorType.UNKNOWN, () => {
      return IconGenericBoilingFlask
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
    .with(ConnectorType.DELTALAKE_IN, () => {
      return { title: 'DeltaLake In', color: 'secondary' as const }
    })
    .with(ConnectorType.DELTALAKE_OUT, () => {
      return { title: 'DeltaLake Out', color: 'secondary' as const }
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
