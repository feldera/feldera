import { Typography } from '@mui/material'

export const TextIcon = (props: { text: string }) => (
  <Typography variant='caption' sx={{ border: 1, borderRadius: 1, px: 0.5, m: 1 }}>
    {props.text}
  </Typography>
)
