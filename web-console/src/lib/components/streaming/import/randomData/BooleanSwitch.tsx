// A boolean switch element for use in forms.

import { Stack, Switch, SwitchProps, Typography } from '@mui/material'

export const BooleanSwitch = (props: SwitchProps) => {
  return (
    <Stack direction='row' spacing={1} alignItems='center'>
      <Typography>False</Typography>
      <Switch {...props} />
      <Typography>True</Typography>
    </Stack>
  )
}
