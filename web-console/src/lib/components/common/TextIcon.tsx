import { Typography, TypographyProps } from '@mui/material'

export const TextIcon = ({ text, ...props }: { text: string; size: number } & TypographyProps) => (
  <Typography
    sx={{
      border: 1,
      borderRadius: 1,
      px: 0.5,
      height: props.size,
      width: props.size,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center'
    }}
    {...props}
  >
    {text}
  </Typography>
)
