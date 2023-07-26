// Adds a slider for quantile selection to the regular data-grid pagination
// mechanism.

import { Dispatch, SetStateAction } from 'react'
import { GridPagination } from '@mui/x-data-grid-pro'
import { Box, Grid, Slider, Typography } from '@mui/material'

interface PercentilePaginationProps {
  showSlider: boolean
  setQuantile: Dispatch<SetStateAction<number>>
  quantile: number
  onQuantileSelected: (event: React.SyntheticEvent | Event, value: number | number[]) => void
}

export const PercentilePagination = (props: any) => {
  const { showSlider, quantile, setQuantile, onQuantileSelected, ...otherProps } = props as PercentilePaginationProps
  const onChange = (event: React.SyntheticEvent | Event, value: number | number[]) => {
    setQuantile(value as number)
  }

  return (
    <>
      {showSlider ? (
        <Grid container justifyContent='center' alignItems='center'>
          <Grid item xs={3}></Grid>
          <Grid item xs={3}>
            <Typography align='center'>Jump to percentile:</Typography>
          </Grid>
          <Grid item xs={6}>
            <Box mt={1.5}>
              <Slider
                marks
                step={5}
                value={quantile}
                valueLabelDisplay='auto'
                onChange={onChange}
                onChangeCommitted={onQuantileSelected}
              />
            </Box>
          </Grid>
        </Grid>
      ) : (
        <></>
      )}
      <GridPagination {...otherProps} />
    </>
  )
}
