import { fromLibrdkafkaConfig, librdkafkaAuthOptions } from '$lib/functions/kafka/librdkafkaOptions'
import * as va from 'valibot'

const sslSchema = va.object({
  ssl_key_pem: va.optional(va.string()),
  ssl_certificate_pem: va.optional(va.string()),
  enable_ssl_certificate_verification: va.optional(va.boolean(), true),
  ssl_ca_pem: va.optional(va.string())
})

const saslPassSchema = va.object({
  sasl_mechanism: va.optional(
    va.picklist(['GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']),
    'PLAIN'
  ),
  sasl_username: va.optional(va.string()),
  sasl_password: va.optional(va.string())
})

const saslOauthSchema = va.intersect([
  va.object({
    sasl_mechanism: va.literal('OAUTHBEARER'),
    sasl_oauthbearer_config: va.optional(va.string()),
    enable_sasl_oauthbearer_unsecure_jwt: va.optional(va.boolean(), false)
  }),
  va.union([
    va.object({
      sasl_oauthbearer_method: va.optional(va.literal('default'), 'default')
    }),
    va.object({
      sasl_oauthbearer_method: va.literal('oidc'),
      sasl_oauthbearer_client_id: va.string([va.minLength(1)]),
      sasl_oauthbearer_client_secret: va.string([va.minLength(1)]),
      sasl_oauthbearer_token_endpoint_url: va.string([va.minLength(1)]),
      sasl_oauthbearer_scope: va.optional(va.string()),
      sasl_oauthbearer_extensions: va.optional(va.string())
    })
  ])
])

const saslGenericSchema = va.object({
  sasl_mechanism: va.string()
})

const saslPlaintextSchema = va.union([saslPassSchema, saslOauthSchema, saslGenericSchema])

export const authParamsSchema = va.union([
  va.object({
    security_protocol: va.literal('PLAINTEXT')
  }),
  va.merge([
    va.object({
      security_protocol: va.literal('SSL')
    }),
    sslSchema
  ]),
  va.intersect([
    va.object({
      security_protocol: va.literal('SASL_PLAINTEXT')
    }),
    saslPlaintextSchema
  ]),
  va.intersect([
    va.object({
      security_protocol: va.literal('SASL_SSL')
    }),
    sslSchema,
    saslPlaintextSchema
  ]),
  va.object({
    security_protocol: va.optional(va.string())
  })
])

export const authFields = librdkafkaAuthOptions.map((option) => option.replaceAll('.', '_'))

export type KafkaAuthSchema = va.Input<typeof authParamsSchema>

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
