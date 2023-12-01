import { GridItems } from '$lib/components/common/GridItems'
import { KafkaAuthSchema } from '$lib/functions/kafka/authParamsSchema'
import {
  PasswordElement,
  SelectElement,
  SwitchElement,
  TextareaAutosizeElement,
  TextFieldElement,
  useFormContext
} from 'react-hook-form-mui'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'

import { Grid, Tooltip } from '@mui/material'

const SaslOauthOidcForm = (props: { disabled?: boolean }) => (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <TextFieldElement
        name='config.sasl_oauthbearer_client_id'
        label='sasl.oauthbearer.client.id'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-sasl-oauthbearer-client-id'
        }}
      />
      <TextFieldElement
        name='config.sasl_oauthbearer_client_secret'
        label='sasl.oauthbearer.client.secret'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-sasl-oauthbearer-client-secret'
        }}
      />
      <TextFieldElement
        name='config.sasl_oauthbearer_token_endpoint_url'
        label='sasl.oauthbearer.token.endpoint.url'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-sasl-oauthbearer-token-endpoint-url'
        }}
      />
      <TextFieldElement
        name='config.sasl_oauthbearer_scope'
        label='sasl.oauthbearer.scope'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-sasl-oauthbearer-scope'
        }}
      />
      <TextFieldElement
        name='config.sasl_oauthbearer_extensions'
        label='sasl.oauthbearer.extensions'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-sasl-oauthbearer-extensions'
        }}
      />
    </GridItems>
  </Grid>
)

const SaslOauthForm = (props: { disabled?: boolean }) => {
  const auth = useFormContext<{ config: KafkaAuthSchema }>().watch('config')
  invariant(
    (auth['security_protocol'] === 'SASL_PLAINTEXT' || auth['security_protocol'] === 'SASL_SSL') &&
      auth['sasl_mechanism'] === 'OAUTHBEARER'
  )
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <TextFieldElement
          name='config.sasl_oauthbearer_config'
          label='sasl.oauthbearer.config'
          size='small'
          placeholder=''
          aria-describedby='validation-host'
          fullWidth
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-sasl-oauthbearer-config'
          }}
        />
        <SwitchElement
          name='config.enable_sasl_oauthbearer_unsecure_jwt'
          label='enable.sasl.oauthbearer.unsecure.jwt'
          disabled={props.disabled}
        />
        <SelectElement
          name='config.sasl_oauthbearer_method'
          label='sasl.oauthbearer.method'
          size='small'
          options={[
            {
              id: 'default',
              label: 'default'
            },
            {
              id: 'oidc',
              label: 'oidc'
            }
          ]}
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-sasl-oauthbearer-method'
          }}
        ></SelectElement>
        {match(auth['sasl_oauthbearer_method'])
          .with('oidc', () => <SaslOauthOidcForm disabled={props.disabled} />)
          .otherwise(() => (
            <></>
          ))}
      </GridItems>
    </Grid>
  )
}

const SaslPassForm = (props: { disabled?: boolean }) => (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <TextFieldElement
        name='config.sasl_username'
        label='sasl.username'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-sasl-username'
        }}
      />
      <PasswordElement
        name='config.sasl_password'
        label='sasl.password'
        size='small'
        fullWidth
        disabled={props.disabled}
      />
    </GridItems>
  </Grid>
)

const SaslForm = (props: { disabled?: boolean }) => {
  const auth = useFormContext<{ config: KafkaAuthSchema }>().watch('config')
  invariant(auth['security_protocol'] === 'SASL_PLAINTEXT' || auth['security_protocol'] === 'SASL_SSL')

  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <Tooltip title='SASL mechanism to use for authentication.' placement='right'>
          <div>
            <SelectElement
              name='config.sasl_mechanism'
              label='sasl.mechanism'
              size='small'
              options={[
                {
                  id: 'GSSAPI',
                  label: 'GSSAPI',
                  disabled: true
                },
                {
                  id: 'PLAIN',
                  label: 'PLAIN'
                },
                {
                  id: 'SCRAM-SHA-256',
                  label: 'SCRAM-SHA-256'
                },
                {
                  id: 'SCRAM-SHA-512',
                  label: 'SCRAM-SHA-512'
                },
                {
                  id: 'OAUTHBEARER',
                  label: 'OAUTHBEARER',
                  disabled: true
                }
              ]}
              disabled={props.disabled}
              inputProps={{
                'data-testid': 'input-sasl-mechanism'
              }}
            ></SelectElement>
          </div>
        </Tooltip>
        {match(auth['sasl_mechanism'])
          .with('OAUTHBEARER', () => <SaslOauthForm disabled={props.disabled} />)
          .with('GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', undefined, () => (
            <SaslPassForm disabled={props.disabled} />
          ))
          .exhaustive()}
      </GridItems>
    </Grid>
  )
}

const SslForm = (props: { disabled?: boolean }) => (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <Tooltip title="Client's private key string (PEM format) used for authentication.">
        <div>
          <TextareaAutosizeElement
            name='config.ssl_key_pem'
            label='ssl.key.pem'
            size='small'
            placeholder=''
            aria-describedby='validation-host'
            fullWidth
            resizeStyle='vertical'
            disabled={props.disabled}
            inputProps={{
              'data-testid': 'input-ssl-key-pem'
            }}
          />
        </div>
      </Tooltip>
      <Tooltip title="Client's public key string (PEM format) used for authentication.">
        <div>
          <TextareaAutosizeElement
            name='config.ssl_certificate_pem'
            label='ssl.certificate.pem'
            size='small'
            placeholder=''
            aria-describedby='validation-host'
            fullWidth
            resizeStyle='vertical'
            disabled={props.disabled}
            inputProps={{
              'data-testid': 'input-ssl-certificate-pem'
            }}
          />
        </div>
      </Tooltip>
      <Tooltip title="Enable OpenSSL's builtin broker (server) certificate verification." placement='right'>
        <div>
          <SwitchElement
            name='config.enable_ssl_certificate_verification'
            label='enable.ssl.certificate.verification'
            disabled={props.disabled}

            data-testid='input-enable-ssl-certificate-verification'
          />
        </div>
      </Tooltip>
      <Tooltip title="CA certificate string (PEM format) for verifying the broker's key.">
        <div>
          <TextareaAutosizeElement
            name='config.ssl_ca_pem'
            label='ssl.ca.pem'
            size='small'
            placeholder=''
            aria-describedby='validation-host'
            fullWidth
            resizeStyle='vertical'
            disabled={props.disabled}
            inputProps={{
              'data-testid': 'input-ssl-ca-pem'
            }}
          />
        </div>
      </Tooltip>
    </GridItems>
  </Grid>
)

const securityProtocolDesc = (p: KafkaAuthSchema['security_protocol']) =>
  match(p)
    .with('PLAINTEXT', undefined, () => 'Un-authenticated, non-encrypted channel')
    .with('SASL_PLAINTEXT', () => 'SASL authenticated, non-encrypted channel')
    .with('SASL_SSL', () => 'SASL authenticated, SSL channel')
    .with('SSL', () => 'SSL channel')
    .exhaustive()

export const TabKafkaAuth = (props: { disabled?: boolean }) => {
  const auth = useFormContext<{ config: KafkaAuthSchema }>().watch('config')
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <SelectElement
          name='config.security_protocol'
          value='security_protocol'
          label='security.protocol'
          size='small'
          helperText={securityProtocolDesc(auth['security_protocol'])}
          options={[
            {
              id: 'PLAINTEXT',
              label: 'PLAINTEXT'
            },
            {
              id: 'SSL',
              label: 'SSL'
            },
            {
              id: 'SASL_PLAINTEXT',
              label: 'SASL_PLAINTEXT'
            },
            {
              id: 'SASL_SSL',
              label: 'SASL_SSL'
            }
          ]}
          disabled={props.disabled}
          inputProps={{
            'data-testid': 'input-security-protocol'
          }}
        ></SelectElement>
        {match(auth['security_protocol'])
          .with('PLAINTEXT', undefined, () => <></>)
          .with('SSL', () => <SslForm disabled={props.disabled} />)
          .with('SASL_PLAINTEXT', () => <SaslForm disabled={props.disabled} />)
          .with('SASL_SSL', () => (
            <>
              <SslForm disabled={props.disabled} />
              <SaslForm disabled={props.disabled} />
            </>
          ))
          .exhaustive()}
      </GridItems>
    </Grid>
  )
}
