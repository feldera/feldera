import { TextFieldElement } from 'react-hook-form-mui'

export const KafkaTopicElement = (props: { disabled?: boolean; parentName: string }) => (
  <TextFieldElement
    name={props.parentName + '.topic'}
    label='Topic Name'
    size='small'
    fullWidth
    placeholder='my-topic'
    aria-describedby='validation-topic'
    disabled={props.disabled}
    inputProps={{
      'data-testid': 'input-topic'
    }}
  />
)
