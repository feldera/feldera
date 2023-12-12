import { match } from 'ts-pattern'
import * as va from 'valibot'

const sslSchema = va.object({
  ssl_key_pem: va.optional(va.string()),
  ssl_certificate_pem: va.optional(va.string()),
  enable_ssl_certificate_verification: va.optional(va.boolean(), true),
  ssl_ca_pem: va.optional(va.string())
})

const saslPassSchema = va.object({
  sasl_mechanism: va.optional(va.enumType(['GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']), 'PLAIN'),
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

const saslPlaintextSchema = va.union([saslPassSchema, saslOauthSchema])

export const authParamsSchema = va.union([
  va.object({
    security_protocol: va.optional(va.literal('PLAINTEXT'), 'PLAINTEXT')
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
  ])
])

export const authFields = [
  'security_protocol',
  'ssl_key_pem',
  'ssl_certificate_pem',
  'enable_ssl_certificate_verification',
  'ssl_ca_pem',
  'sasl_mechanism',
  'sasl_username',
  'sasl_password',
  'sasl_oauthbearer_config',
  'enable_sasl_oauthbearer_unsecure_jwt',
  'sasl_oauthbearer_method',
  'sasl_oauthbearer_client_id',
  'sasl_oauthbearer_client_secret',
  'sasl_oauthbearer_token_endpoint_url',
  'sasl_oauthbearer_scope',
  'sasl_oauthbearer_extensions'
] as const

export type KafkaAuthSchema = va.Input<typeof authParamsSchema>

type SaslSchema<T extends KafkaAuthSchema = KafkaAuthSchema> = T extends { security_protocol: 'SASL_PLAINTEXT' }
  ? T
  : T extends { security_protocol: 'SASL_SSL' }
    ? T
    : never

const prepareSaslData = (data: SaslSchema) => ({
  'sasl.mechanism': data.sasl_mechanism,
  ...match(data)
    .with(
      { sasl_mechanism: 'GSSAPI' },
      { sasl_mechanism: 'PLAIN' },
      { sasl_mechanism: 'SCRAM-SHA-256' },
      { sasl_mechanism: 'SCRAM-SHA-512' },
      { sasl_mechanism: undefined },
      data => ({
        'sasl.username': data.sasl_username,
        'sasl.password': data.sasl_password
      })
    )
    .with({ sasl_mechanism: 'OAUTHBEARER' }, data => ({
      'sasl.oauthbearer.config': data.sasl_oauthbearer_config,
      'enable.sasl.oauthbearer.unsecure.jwt': (v => (v === undefined ? v : String(v)))(
        data.enable_sasl_oauthbearer_unsecure_jwt
      ),
      'sasl.oauthbearer.method': data.sasl_oauthbearer_method,
      ...match(data)
        .with({ sasl_oauthbearer_method: 'default' }, { sasl_oauthbearer_method: undefined }, () => ({}))
        .with({ sasl_oauthbearer_method: 'oidc' }, data => ({
          'sasl.oauthbearer.client.id': data.sasl_oauthbearer_client_id,
          'sasl.oauthbearer.client.secret': data.sasl_oauthbearer_client_secret,
          'sasl.oauthbearer.token.endpoint.url': data.sasl_oauthbearer_token_endpoint_url,
          'sasl.oauthbearer.scope': data.sasl_oauthbearer_scope,
          'sasl.oauthbearer.extensions': data.sasl_oauthbearer_extensions
        }))
        .exhaustive()
    }))
    .exhaustive()
})

type SslSchema<T extends KafkaAuthSchema = KafkaAuthSchema> = T extends { security_protocol: 'SSL' }
  ? T
  : T extends { security_protocol: 'SASL_SSL' }
    ? T
    : never

const prepareSslData = (data: SslSchema) => ({
  'ssl.key.pem': data.ssl_key_pem,
  'ssl.certificate.pem': data.ssl_certificate_pem,
  'enable.ssl.certificate.verification': (v => (v === undefined ? v : String(v)))(
    data.enable_ssl_certificate_verification
  ),
  'ssl.ca.pem': data.ssl_ca_pem
})

export const prepareAuthData = (data: KafkaAuthSchema) => ({
  'security.protocol': data.security_protocol,
  ...match(data)
    .with({ security_protocol: 'PLAINTEXT' }, { security_protocol: undefined }, () => ({}))
    .with({ security_protocol: 'SSL' }, prepareSslData)
    .with({ security_protocol: 'SASL_PLAINTEXT' }, prepareSaslData)
    .with({ security_protocol: 'SASL_SSL' }, data => ({ ...prepareSslData(data), ...prepareSaslData(data) }))
    .exhaustive()
})

export const defaultUiAuthParams = {
  security_protocol: 'PLAINTEXT',
  enable_ssl_certificate_verification: true,
  sasl_mechanism: 'PLAIN',
  sasl_oauthbearer_method: 'default'
} as KafkaAuthSchema

export const parseAuthParams = (config: Record<string, unknown>) => {
  if (typeof config['security.protocol'] === 'string') {
    config['security.protocol'] = config['security.protocol'].toUpperCase()
  }
  return {
    ...defaultUiAuthParams,
    ...va.parse(
      authParamsSchema,
      Object.fromEntries(
        Object.entries(config).map(([k, v]) => [
          k.replaceAll('.', '_'),
          v === 'true' ? true : v === 'false' ? false : v
        ])
      )
    )
  }
}
