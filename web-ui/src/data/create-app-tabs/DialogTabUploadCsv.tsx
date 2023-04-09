// ** MUI Imports
import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import Switch from '@mui/material/Switch'
import FormGroup from '@mui/material/FormGroup'
import FormControlLabel from '@mui/material/FormControlLabel'

import FileUploaderRestrictions from 'src/views/forms/form-elements/file-uploader/FileUploaderRestrictions'

const TabDetails = () => {
  return (
    <Grid container spacing={5}>
      <Grid item xs={12} spacing={6}>
        <Typography variant='h6'>CSV Settings</Typography>
        <FormGroup row>
          <FormControlLabel control={<Switch />} label='First Row is Header Row' />
        </FormGroup>{' '}
      </Grid>
      <Grid item xs={12} spacing={6}>
        <Typography variant='h6'>Upload Files</Typography>
      </Grid>
      <Grid item>
        <FileUploaderRestrictions />
      </Grid>
    </Grid>
  )
}

export default TabDetails
