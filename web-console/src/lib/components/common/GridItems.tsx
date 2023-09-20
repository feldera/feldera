import flattenChildren from 'react-keyed-flatten-children'

import { Grid, GridProps } from '@mui/material'

/**
 * Wraps each child with \<Grid item /\> element with the same props
 * @param props
 * @returns
 */
export const GridItems = (props: GridProps) => {
  return flattenChildren(props.children).map((element, index) => (
    <Grid item {...props} key={index}>
      {element}
    </Grid>
  ))
}
