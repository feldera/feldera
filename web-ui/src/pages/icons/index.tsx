// ** MUI Imports
import Grid from '@mui/material/Grid'
import Card from '@mui/material/Card'
import Link from '@mui/material/Link'
import Button from '@mui/material/Button'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import CardContent from '@mui/material/CardContent'

/**
 ** Icons Imports:
 * ! You need to import all the icons which come from the API or from your server and then add these icons in 'icons' variable.
 * ! If you need all the icons from the library, use "import * as Icon from 'mdi-material-ui'"
 * */
import Abacus from 'mdi-material-ui/Abacus'
import Account from 'mdi-material-ui/Account'
import AbTesting from 'mdi-material-ui/AbTesting'
import AccountBox from 'mdi-material-ui/AccountBox'
import AccountCog from 'mdi-material-ui/AccountCog'
import AbjadArabic from 'mdi-material-ui/AbjadArabic'
import AbjadHebrew from 'mdi-material-ui/AbjadHebrew'
import AbugidaThai from 'mdi-material-ui/AbugidaThai'
import AccessPoint from 'mdi-material-ui/AccessPoint'
import AccountCash from 'mdi-material-ui/AccountCash'
import AccountEdit from 'mdi-material-ui/AccountEdit'
import AccountAlert from 'mdi-material-ui/AccountAlert'
import AccountCheck from 'mdi-material-ui/AccountCheck'
import AccountChild from 'mdi-material-ui/AccountChild'
import AccountClock from 'mdi-material-ui/AccountClock'
import AccountGroup from 'mdi-material-ui/AccountGroup'
import AccountCancel from 'mdi-material-ui/AccountCancel'
import AccountCircle from 'mdi-material-ui/AccountCircle'
import AccessPointOff from 'mdi-material-ui/AccessPointOff'
import AccountConvert from 'mdi-material-ui/AccountConvert'
import AccountDetails from 'mdi-material-ui/AccountDetails'
import AccessPointPlus from 'mdi-material-ui/AccessPointPlus'
import AccessPointCheck from 'mdi-material-ui/AccessPointCheck'
import AccessPointMinus from 'mdi-material-ui/AccessPointMinus'
import AccountArrowLeft from 'mdi-material-ui/AccountArrowLeft'
import AccountCowboyHat from 'mdi-material-ui/AccountCowboyHat'
import AbugidaDevanagari from 'mdi-material-ui/AbugidaDevanagari'
import AccessPointRemove from 'mdi-material-ui/AccessPointRemove'
import AccountArrowRight from 'mdi-material-ui/AccountArrowRight'
import AccountBoxOutline from 'mdi-material-ui/AccountBoxOutline'
import AccountCogOutline from 'mdi-material-ui/AccountCogOutline'
import AccessPointNetwork from 'mdi-material-ui/AccessPointNetwork'
import AccountBoxMultiple from 'mdi-material-ui/AccountBoxMultiple'
import AccountCashOutline from 'mdi-material-ui/AccountCashOutline'
import AccountChildCircle from 'mdi-material-ui/AccountChildCircle'
import AccountEditOutline from 'mdi-material-ui/AccountEditOutline'
import AccountAlertOutline from 'mdi-material-ui/AccountAlertOutline'
import AccountCheckOutline from 'mdi-material-ui/AccountCheckOutline'
import AccountChildOutline from 'mdi-material-ui/AccountChildOutline'
import AccountClockOutline from 'mdi-material-ui/AccountClockOutline'
import AccountCancelOutline from 'mdi-material-ui/AccountCancelOutline'
import AccountCircleOutline from 'mdi-material-ui/AccountCircleOutline'
import AccessPointNetworkOff from 'mdi-material-ui/AccessPointNetworkOff'
import AccountConvertOutline from 'mdi-material-ui/AccountConvertOutline'
import AccountDetailsOutline from 'mdi-material-ui/AccountDetailsOutline'
import AccountArrowLeftOutline from 'mdi-material-ui/AccountArrowLeftOutline'
import AccountArrowRightOutline from 'mdi-material-ui/AccountArrowRightOutline'
import AccountBoxMultipleOutline from 'mdi-material-ui/AccountBoxMultipleOutline'

const icons = {
  Abacus,
  Account,
  AbTesting,
  AccountBox,
  AccountCog,
  AbjadArabic,
  AbjadHebrew,
  AbugidaThai,
  AccessPoint,
  AccountCash,
  AccountEdit,
  AccountAlert,
  AccountCheck,
  AccountChild,
  AccountClock,
  AccountGroup,
  AccountCancel,
  AccountCircle,
  AccessPointOff,
  AccountConvert,
  AccountDetails,
  AccessPointPlus,
  AccessPointCheck,
  AccessPointMinus,
  AccountArrowLeft,
  AccountCowboyHat,
  AbugidaDevanagari,
  AccessPointRemove,
  AccountArrowRight,
  AccountBoxOutline,
  AccountCogOutline,
  AccessPointNetwork,
  AccountBoxMultiple,
  AccountCashOutline,
  AccountChildCircle,
  AccountEditOutline,
  AccountAlertOutline,
  AccountCheckOutline,
  AccountChildOutline,
  AccountClockOutline,
  AccountCancelOutline,
  AccountCircleOutline,
  AccessPointNetworkOff,
  AccountConvertOutline,
  AccountDetailsOutline,
  AccountArrowLeftOutline,
  AccountArrowRightOutline,
  AccountBoxMultipleOutline
}

const Icons = () => {
  const renderIconGrids = () => {
    return Object.keys(icons).map(key => {
      const IconTag = icons[key as keyof typeof icons]

      return (
        <Grid item key={key}>
          <Tooltip arrow title={key} placement='top'>
            <Card>
              <CardContent sx={{ display: 'flex' }}>
                <IconTag />
              </CardContent>
            </Card>
          </Tooltip>
        </Grid>
      )
    })
  }

  return (
    <Grid container spacing={6}>
      <Grid item xs={12}>
        <Typography variant='h5'>
          <Link href='https://materialdesignicons.com/' target='_blank'>
            Material Design Icons
          </Link>
        </Typography>
        <Typography variant='body2'>Material Design Icons from the Community</Typography>
      </Grid>
      <Grid item xs={12}>
        <Grid container spacing={6}>
          {renderIconGrids()}
        </Grid>
      </Grid>
      <Grid item xs={12} sx={{ textAlign: 'center' }}>
        <Button
          target='_blank'
          rel='noreferrer'
          component={Link}
          variant='contained'
          href='https://materialdesignicons.com/'
        >
          View All Material Design Icons
        </Button>
      </Grid>
    </Grid>
  )
}

export default Icons
