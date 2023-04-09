import List from '@mui/material/List'
import Divider from '@mui/material/Divider'
import ListItem from '@mui/material/ListItem'
import ListItemIcon from '@mui/material/ListItemIcon'
import Typography from '@mui/material/Typography'
import ListItemText from '@mui/material/ListItemText'
import ListItemButton from '@mui/material/ListItemButton'
import ListItemSecondaryAction from '@mui/material/ListItemSecondaryAction'
import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import CardHeader from '@mui/material/CardHeader'

import { Icon } from '@iconify/react'

const Health = () => {
  return (
    <Card>
      <CardHeader title='DBSP Health'></CardHeader>

      <CardContent>
        <List component='nav' aria-label='main mailbox'>
          <ListItem disablePadding>
            <ListItemButton>
              <ListItemIcon>
                <Icon icon='bx:error-circle' fontSize={20} />
              </ListItemIcon>
              <ListItemText primary='Reported errors' />
              <ListItemSecondaryAction>
                <Typography variant='h6'>0</Typography>
              </ListItemSecondaryAction>
            </ListItemButton>
          </ListItem>
          <ListItem disablePadding>
            <ListItemButton>
              <ListItemIcon>
                <Icon icon='bx:error-circle' fontSize={20} />
              </ListItemIcon>
              <ListItemText primary='Reported warnings' />
              <ListItemSecondaryAction>
                <Typography variant='h6'>0</Typography>
              </ListItemSecondaryAction>
            </ListItemButton>
          </ListItem>
        </List>
        <Divider sx={{ m: '0 !important' }} />
        <List component='nav' aria-label='secondary mailbox'>
          <ListItem disablePadding>
            <ListItemButton>
              <ListItemIcon>
                <Icon icon='bx:time-five' fontSize={20} />
              </ListItemIcon>
              <ListItemText primary='Other notifications' />
              <ListItemSecondaryAction>
                <Typography variant='h6'>3</Typography>
              </ListItemSecondaryAction>
            </ListItemButton>
          </ListItem>
        </List>
      </CardContent>
    </Card>
  )
}

export default Health
