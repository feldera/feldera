import Grid from '@mui/material/Grid'
import TextField from '@mui/material/TextField'
import Select from '@mui/material/Select'
import MenuItem from '@mui/material/MenuItem'
import InputLabel from '@mui/material/InputLabel'
import FormControl from '@mui/material/FormControl'
import FormHelperText from '@mui/material/FormHelperText'
import Typography from '@mui/material/Typography'

const TypeMenu = () => {
  return (
    <FormControl>
      <InputLabel>Type</InputLabel>
      <Select>
        <MenuItem value={10}>Integer</MenuItem>
        <MenuItem value={20}>String</MenuItem>
        <MenuItem value={30}>Boolean</MenuItem>
      </Select>
    </FormControl>
  )
}

const DialogTabDataFormat = () => {
  return (
    <Grid container spacing={6}>
      <Grid item>
        <FormControl>
          <InputLabel>Select Format</InputLabel>
          <Select>
            <MenuItem value=''>
              <em>New type</em>
            </MenuItem>
            <MenuItem value={10}>Ticker Data</MenuItem>
            <MenuItem value={20}>Close/Open</MenuItem>
            <MenuItem value={30}>Option Prices</MenuItem>
          </Select>
          <FormHelperText>Select an already existing Data Format.</FormHelperText>
        </FormControl>
      </Grid>

      <Grid item xs={12} spacing={6}>
        <Typography variant='body1'>Or define a new Data Format</Typography>
      </Grid>

      <Grid item xs={12} sm={8}>
        <TextField fullWidth name='name' autoComplete='on' label='Name' placeholder='id' />
      </Grid>
      <Grid item sm={4}>
        <TypeMenu />
      </Grid>
      <Grid item sm={8}>
        <TextField fullWidth name='name' autoComplete='on' label='Name' placeholder='zip_code' />
      </Grid>
      <Grid item sm={4}>
        <TypeMenu />
      </Grid>
      <Grid item sm={8}>
        <TextField fullWidth name='name' autoComplete='on' label='Name' placeholder='state' />
      </Grid>
      <Grid item sm={4}>
        <TypeMenu />
      </Grid>
    </Grid>
  )
}

export default DialogTabDataFormat
