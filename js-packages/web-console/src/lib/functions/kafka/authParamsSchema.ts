import { fromLibrdkafkaConfig, librdkafkaAuthOptions } from '$lib/functions/kafka/librdkafkaOptions'
import * as va from 'valibot'

const sslSchema = va.object({
  ssl_key_pem: va.optional(va.string()),
  ssl_certificate_pem: va.optional(va.string()),
  enable_ssl_certificate_verification: va.optional(va.boolean(), true),
  ssl_ca_pem: va.optional(va.string())
})

const saslPassSchema = <T extends string>(security_protocol: va.LiteralSchema<T, undefined>) =>
  va.object({
    security_protocol,
    sasl_mechanism: va.picklist(['GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']),
    sasl_username: va.optional(va.string()),
    sasl_password: va.optional(va.string())
  })

const saslOauthCommon = va.object({
  sasl_mechanism: va.literal('OAUTHBEARER'),
  sasl_oauthbearer_config: va.optional(va.string()),
  enable_sasl_oauthbearer_unsecure_jwt: va.optional(va.boolean(), false)
})

const saslOauthSchema = <
  T extends string,
  E extends va.ObjectEntries,
  M extends va.ErrorMessage<va.ObjectIssue> | undefined
>(
  security_protocol: va.LiteralSchema<T, undefined>,
  extra?: va.ObjectSchema<E, M>
) =>
  va.variant('sasl_oauthbearer_method', [
    va.object({
      security_protocol,
      ...saslOauthCommon.entries,
      sasl_oauthbearer_method: va.literal('default'),
      ...extra?.entries
    }),
    va.object({
      security_protocol,
      ...saslOauthCommon.entries,
      sasl_oauthbearer_method: va.literal('oidc'),
      sasl_oauthbearer_client_id: va.pipe(va.string(), va.minLength(1)),
      sasl_oauthbearer_client_secret: va.pipe(va.string(), va.minLength(1)),
      sasl_oauthbearer_token_endpoint_url: va.pipe(va.string(), va.minLength(1)),
      sasl_oauthbearer_scope: va.optional(va.string()),
      sasl_oauthbearer_extensions: va.optional(va.string()),
      ...extra?.entries
    })
  ])

const saslGenericSchema = <T extends string>(security_protocol: va.LiteralSchema<T, undefined>) =>
  va.object({
    security_protocol,
    sasl_mechanism: va.string()
  })

const saslPlaintextSchema = <
  T extends string,
  E extends va.ObjectEntries,
  M extends va.ErrorMessage<va.ObjectIssue> | undefined
>(
  security_protocol: va.LiteralSchema<T, undefined>,
  extra?: va.ObjectSchema<E, M>
) =>
  va.variant('sasl_mechanism', [
    va.object({
      ...saslPassSchema(security_protocol).entries,
      ...extra?.entries
    }),
    saslOauthSchema(security_protocol, extra),
    va.object({
      ...saslGenericSchema(security_protocol).entries,
      ...extra?.entries
    })
  ])

export const authParamsSchema = va.variant('security_protocol', [
  va.object({
    security_protocol: va.literal('PLAINTEXT')
  }),
  va.object({
    security_protocol: va.literal('SSL'),
    ...sslSchema.entries
  }),
  saslPlaintextSchema(va.literal('SASL_PLAINTEXT')),
  saslPlaintextSchema(
    va.literal('SASL_SSL'),
    va.object({
      sslSchema
    })
  ),
  va.object({
    security_protocol: va.string()
  })
])

export const authFields = librdkafkaAuthOptions.map((option) => option.replaceAll('.', '_'))

export type KafkaAuthSchema = va.InferInput<typeof authParamsSchema>

export const defaultLibrdkafkaAuthOptions = {
  security_protocol: 'PLAINTEXT',
  enable_ssl_certificate_verification: true,
  sasl_mechanism: 'PLAIN'
} as KafkaAuthSchema

export const parseAuthParams = (config: Record<string, string>) => {
  if (typeof config['security.protocol'] === 'string') {
    config['security.protocol'] = config['security.protocol'].toUpperCase()
  }
  return {
    ...defaultLibrdkafkaAuthOptions,
    ...va.parse(authParamsSchema, fromLibrdkafkaConfig(config))
  }
}
