import Markdown from 'react-markdown'

import { Tooltip, TooltipProps, useTheme } from '@mui/material'

export const FormTooltip = (props: TooltipProps & { title?: string }) => {
  const theme = useTheme()
  return (
    <Tooltip
      {...props}
      slotProps={{
        tooltip: {
          sx: {
            backgroundColor: theme.palette.background.default,
            color: theme.palette.text.primary,
            fontSize: 14
          }
        }
      }}
      title={props.title ? <Markdown>{props.title}</Markdown> : undefined}
      disableInteractive
    />
  )
}
