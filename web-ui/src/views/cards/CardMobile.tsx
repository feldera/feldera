// ** React Imports
import { MouseEvent, useState } from 'react'

// ** MUI Imports
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Menu from '@mui/material/Menu'
import Button from '@mui/material/Button'
import MenuItem from '@mui/material/MenuItem'
import { styled } from '@mui/material/styles'
import Typography from '@mui/material/Typography'
import IconButton from '@mui/material/IconButton'
import CardContent from '@mui/material/CardContent'
import CardActions from '@mui/material/CardActions'
import Grid, { GridProps } from '@mui/material/Grid'

// ** Icons Imports
import Twitter from 'mdi-material-ui/Twitter'
import CartPlus from 'mdi-material-ui/CartPlus'
import Facebook from 'mdi-material-ui/Facebook'
import Linkedin from 'mdi-material-ui/Linkedin'
import GooglePlus from 'mdi-material-ui/GooglePlus'
import ShareVariant from 'mdi-material-ui/ShareVariant'

// Styled Grid component
const StyledGrid = styled(Grid)<GridProps>(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  [theme.breakpoints.down('md')]: {
    borderBottom: `1px solid ${theme.palette.divider}`
  },
  [theme.breakpoints.up('md')]: {
    borderRight: `1px solid ${theme.palette.divider}`
  }
}))

const CardMobile = () => {
  // ** State
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null)

  const open = Boolean(anchorEl)

  const handleClick = (event: MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget)
  }
  const handleClose = () => {
    setAnchorEl(null)
  }

  return (
    <Card>
      <Grid container spacing={6}>
        <StyledGrid item md={5} xs={12}>
          <CardContent sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <img width={137} height={176} alt='Apple iPhone 11 Pro' src='/images/cards/iPhone-11-pro.png' />
          </CardContent>
        </StyledGrid>
        <Grid
          item
          xs={12}
          md={7}
          sx={{
            paddingTop: ['0 !important', '0 !important', '1.5rem !important'],
            paddingLeft: ['1.5rem !important', '1.5rem !important', '0 !important']
          }}
        >
          <CardContent>
            <Typography variant='h6' sx={{ marginBottom: 2 }}>
              Apple iPhone 11 Pro
            </Typography>
            <Typography variant='body2' sx={{ marginBottom: 3.5 }}>
              Apple iPhone 11 Pro smartphone. Announced Sep 2019. Features 5.8″ display Apple A13 Bionic
            </Typography>
            <Typography sx={{ fontWeight: 500, marginBottom: 3 }}>
              Price:{' '}
              <Box component='span' sx={{ fontWeight: 'bold' }}>
                $899
              </Box>
            </Typography>
          </CardContent>
          <CardActions className='card-action-dense'>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', width: '100%' }}>
              <Button>
                <CartPlus fontSize='small' sx={{ marginRight: 2 }} />
                Add to Card
              </Button>
              <IconButton
                id='long-button'
                aria-label='share'
                aria-haspopup='true'
                onClick={handleClick}
                aria-controls='long-menu'
                aria-expanded={open ? 'true' : undefined}
              >
                <ShareVariant fontSize='small' />
              </IconButton>
              <Menu
                open={open}
                id='long-menu'
                anchorEl={anchorEl}
                onClose={handleClose}
                MenuListProps={{
                  'aria-labelledby': 'long-button'
                }}
              >
                <MenuItem onClick={handleClose}>
                  <Facebook />
                </MenuItem>
                <MenuItem onClick={handleClose}>
                  <Twitter />
                </MenuItem>
                <MenuItem onClick={handleClose}>
                  <Linkedin />
                </MenuItem>
                <MenuItem onClick={handleClose}>
                  <GooglePlus />
                </MenuItem>
              </Menu>
            </Box>
          </CardActions>
        </Grid>
      </Grid>
    </Card>
  )
}

export default CardMobile
