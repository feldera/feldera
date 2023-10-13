export enum ConnectorType {
  KAFKA_IN = 'KafkaIn',
  KAFKA_OUT = 'KafkaOut',
  DEBEZIUM_IN = 'DebeziumIn',
  SNOWFLAKE_OUT = 'SnowflakeOut',
  URL = 'HTTP_GET',
  UNKNOWN = 'Unknown'
}

export enum Direction {
  INPUT = 'input',
  OUTPUT = 'output',
  INPUT_OUTPUT = 'input_output'
}
