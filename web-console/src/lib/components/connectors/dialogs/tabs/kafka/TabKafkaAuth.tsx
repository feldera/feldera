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

const saslOauthOidcForm = (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <TextFieldElement
        name='sasl_oauthbearer_client_id'
        label='sasl.oauthbearer.client.id'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
      />
      <TextFieldElement
        name='sasl_oauthbearer_client_secret'
        label='sasl.oauthbearer.client.secret'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
      />
      <TextFieldElement
        name='sasl_oauthbearer_token_endpoint_url'
        label='sasl.oauthbearer.token.endpoint.url'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
      />
      <TextFieldElement
        name='sasl_oauthbearer_scope'
        label='sasl.oauthbearer.scope'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
      />
      <TextFieldElement
        name='sasl_oauthbearer_extensions'
        label='sasl.oauthbearer.extensions'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
      />
    </GridItems>
  </Grid>
)

const SaslOauthForm = () => {
  const auth = useFormContext<KafkaAuthSchema>().watch()
  invariant(
    (auth['security_protocol'] === 'sasl_plaintext' || auth['security_protocol'] === 'sasl_ssl') &&
      auth['sasl_mechanism'] === 'OAUTHBEARER'
  )
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <TextFieldElement
          name='sasl_oauthbearer_config'
          label='sasl.oauthbearer.config'
          size='small'
          placeholder=''
          aria-describedby='validation-host'
          fullWidth
        />
        <SwitchElement name='enable_sasl_oauthbearer_unsecure_jwt' label='enable.sasl.oauthbearer.unsecure.jwt' />
        <SelectElement
          name='sasl_oauthbearer_method'
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
        ></SelectElement>
        {match(auth['sasl_oauthbearer_method'])
          .with('oidc', () => saslOauthOidcForm)
          .otherwise(() => (
            <></>
          ))}
      </GridItems>
    </Grid>
  )
}

const saslPassForm = (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <TextFieldElement
        name='sasl_username'
        label='sasl.username'
        size='small'
        placeholder=''
        aria-describedby='validation-host'
        fullWidth
      />
      <PasswordElement name='sasl_password' label='sasl.password' size='small' fullWidth />
    </GridItems>
  </Grid>
)

const SaslForm = () => {
  const auth = useFormContext<KafkaAuthSchema>().watch()
  invariant(auth['security_protocol'] === 'sasl_plaintext' || auth['security_protocol'] === 'sasl_ssl')

  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <Tooltip title='SASL mechanism to use for authentication.' placement='right'>
          <div>
            <SelectElement
              name='sasl_mechanism'
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
            ></SelectElement>
          </div>
        </Tooltip>
        {match(auth['sasl_mechanism'])
          .with('OAUTHBEARER', () => <SaslOauthForm />)
          .with('GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', undefined, () => saslPassForm)
          .exhaustive()}
      </GridItems>
    </Grid>
  )
}

const sslForm = (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <Tooltip title="Client's private key string (PEM format) used for authentication.">
        <div>
          <TextareaAutosizeElement
            name='ssl_key_pem'
            label='ssl.key.pem'
            size='small'
            placeholder=''
            aria-describedby='validation-host'
            fullWidth
            resizeStyle='vertical'
          />
        </div>
      </Tooltip>
      <Tooltip title="Client's public key string (PEM format) used for authentication.">
        <div>
          <TextareaAutosizeElement
            name='ssl_certificate_pem'
            label='ssl.certificate.pem'
            size='small'
            placeholder=''
            aria-describedby='validation-host'
            fullWidth
            resizeStyle='vertical'
          />
        </div>
      </Tooltip>
      <Tooltip title="Enable OpenSSL's builtin broker (server) certificate verification." placement='right'>
        <div>
          <SwitchElement name='enable_ssl_certificate_verification' label='enable.ssl.certificate.verification' />
        </div>
      </Tooltip>
      <Tooltip title="CA certificate string (PEM format) for verifying the broker's key.">
        <div>
          <TextareaAutosizeElement
            name='ssl_ca_pem'
            label='ssl.ca.pem'
            size='small'
            placeholder=''
            aria-describedby='validation-host'
            fullWidth
            resizeStyle='vertical'
          />
        </div>
      </Tooltip>
    </GridItems>
  </Grid>
)

const securityProtocolDesc = (p: KafkaAuthSchema['security_protocol']) =>
  match(p)
    .with('plaintext', undefined, () => 'Un-authenticated, non-encrypted channel')
    .with('sasl_plaintext', () => 'SASL authenticated, non-encrypted channel')
    .with('sasl_ssl', () => 'SASL authenticated, SSL channel')
    .with('ssl', () => 'SSL channel')
    .exhaustive()

export const TabKafkaAuth = () => {
  const auth = useFormContext<KafkaAuthSchema>().watch()
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <SelectElement
          name='security_protocol'
          value='security_protocol'
          label='security.protocol'
          size='small'
          helperText={securityProtocolDesc(auth['security_protocol'])}
          options={[
            {
              id: 'plaintext',
              label: 'plaintext'
            },
            {
              id: 'ssl',
              label: 'ssl'
            },
            {
              id: 'sasl_plaintext',
              label: 'sasl_plaintext'
            },
            {
              id: 'sasl_ssl',
              label: 'sasl_ssl'
            }
          ]}
        ></SelectElement>
        {match(auth['security_protocol'])
          .with('plaintext', undefined, () => <></>)
          .with('ssl', () => sslForm)
          .with('sasl_plaintext', () => <SaslForm />)
          .with('sasl_ssl', () => (
            <>
              {sslForm}
              <SaslForm />
            </>
          ))
          .exhaustive()}
      </GridItems>
    </Grid>
  )
}
