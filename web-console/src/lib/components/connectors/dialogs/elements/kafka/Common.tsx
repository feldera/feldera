import { TextFieldElement } from 'react-hook-form-mui'

export const KafkaBootstrapServersElement = (props: { disabled?: boolean; parentName: string }) => {
  return (
    <TextFieldElement
      name={props.parentName + '.bootstrap_servers'}
      multiline
      transform={{
        input: (v: string[]) => {
          return v.join(', ')
        },
        output: (v: string) => {
          return v.split(', ')
        }
      }}
      label='bootstrap.servers'
      size='small'
      helperText='Bootstrap Server Hostname, delimited with `, `'
      placeholder='kafka.example.com'
      aria-describedby='validation-host'
      fullWidth
      disabled={props.disabled}
      inputProps={{
        'data-testid': 'input-server-hostname'
      }}
    />
  )
}
