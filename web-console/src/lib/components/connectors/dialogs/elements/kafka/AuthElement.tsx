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
import { match, P } from 'ts-pattern'

import { Box, Grid, Tooltip } from '@mui/material'

const SaslOauthOidcForm = (props: { disabled?: boolean; parentName: string }) => (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <TextFieldElement
        name={props.parentName + '.sasl_oauthbearer_client_id'}
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
        name={props.parentName + '.sasl_oauthbearer_client_secret'}
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
        name={props.parentName + '.sasl_oauthbearer_token_endpoint_url'}
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
        name={props.parentName + '.sasl_oauthbearer_scope'}
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
        name={props.parentName + '.sasl_oauthbearer_extensions'}
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

const SaslOauthForm = (props: { disabled?: boolean; parentName: string }) => {
  const auth: KafkaAuthSchema = useFormContext().watch(props.parentName)
  invariant(
    (auth['security_protocol'] === 'SASL_PLAINTEXT' || auth['security_protocol'] === 'SASL_SSL') &&
      'sasl_mechanism' in auth &&
      auth['sasl_mechanism'] === 'OAUTHBEARER' &&
      'sasl_oauthbearer_method' in auth
  )
  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <TextFieldElement
          name={props.parentName + '.sasl_oauthbearer_config'}
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
          name={props.parentName + '.enable_sasl_oauthbearer_unsecure_jwt'}
          label='enable.sasl.oauthbearer.unsecure.jwt'
          disabled={props.disabled}
        />
        <SelectElement
          name={props.parentName + '.sasl_oauthbearer_method'}
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
          .with('oidc', () => <SaslOauthOidcForm {...props} />)
          .otherwise(() => (
            <></>
          ))}
      </GridItems>
    </Grid>
  )
}

const SaslPassForm = (props: { disabled?: boolean; parentName: string }) => (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <TextFieldElement
        name={props.parentName + '.sasl_username'}
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
        name={props.parentName + '.sasl_password'}
        label='sasl.password'
        size='small'
        fullWidth
        disabled={props.disabled}
      />
    </GridItems>
  </Grid>
)

const SaslForm = (props: { disabled?: boolean; parentName: string }) => {
  const auth: KafkaAuthSchema = useFormContext().watch(props.parentName)
  invariant(
    (auth['security_protocol'] === 'SASL_PLAINTEXT' || auth['security_protocol'] === 'SASL_SSL') &&
      'sasl_mechanism' in auth
  )

  return (
    <Grid container spacing={4}>
      <GridItems xs={12}>
        <Tooltip title='SASL mechanism to use for authentication.' placement='right'>
          <div>
            <SelectElement
              name={props.parentName + '.sasl_mechanism'}
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
          .with('OAUTHBEARER', () => <SaslOauthForm {...props} />)
          .with('GSSAPI', 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', undefined, () => <SaslPassForm {...props} />)
          .with(P.string, () => <>Custom sasl mechanism configuration</>)
          .exhaustive()}
      </GridItems>
    </Grid>
  )
}

const SslForm = (props: { disabled?: boolean; parentName: string }) => (
  <Grid container spacing={4}>
    <GridItems xs={12}>
      <Tooltip title="Client's private key string (PEM format) used for authentication.">
        <div>
          <TextareaAutosizeElement
            name={props.parentName + '.ssl_key_pem'}
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
            name={props.parentName + '.ssl_certificate_pem'}
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
            name={props.parentName + '.enable_ssl_certificate_verification'}
            label='enable.ssl.certificate.verification'
            disabled={props.disabled}
            data-testid='input-enable-ssl-certificate-verification'
          />
        </div>
      </Tooltip>
      <Tooltip title="CA certificate string (PEM format) for verifying the broker's key.">
        <div>
          <TextareaAutosizeElement
            name={props.parentName + '.ssl_ca_pem'}
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
    .with(P.string, () => '')
    .exhaustive()

export const KafkaAuthElement = (props: { disabled?: boolean; parentName: string }) => {
  const auth: KafkaAuthSchema = useFormContext().watch(props.parentName)
  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <SelectElement
        name={props.parentName + '.security_protocol'}
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
        .with('SSL', () => <SslForm {...props} />)
        .with('SASL_PLAINTEXT', () => <SaslForm {...props} />)
        .with('SASL_SSL', () => (
          <>
            <SslForm {...props} />
            <SaslForm {...props} />
          </>
        ))
        .with(P.string, () => <>Custom security protocol configuration</>)
        .exhaustive()}
    </Box>
  )
}
