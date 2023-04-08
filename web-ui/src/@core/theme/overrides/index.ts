// ** MUI Imports
import { Theme } from '@mui/material/styles'

// ** Overrides Imports
import MuiCard from './card'
import MuiChip from './chip'
import MuiLink from './link'
import MuiList from './list'
import MuiMenu from './menu'
import MuiTabs from './tabs'
import MuiInput from './input'
import MuiPaper from './paper'
import MuiTable from './table'
import MuiAlerts from './alerts'
import MuiButton from './button'
import MuiDialog from './dialog'
import MuiRating from './rating'
import MuiSelect from './select'
import MuiAvatar from './avatars'
import MuiDivider from './divider'
import MuiPopover from './popover'
import MuiTooltip from './tooltip'
import MuiBackdrop from './backdrop'
import MuiSnackbar from './snackbar'
import MuiSwitches from './switches'
import MuiTimeline from './timeline'
import MuiAccordion from './accordion'
import MuiPagination from './pagination'
import MuiTypography from './typography'
import MuiToggleButton from './toggleButton'
import MuiDateTimePicker from './dateTimePicker'

const Overrides = (theme: Theme) => {
  const chip = MuiChip(theme)
  const list = MuiList(theme)
  const menu = MuiMenu(theme)
  const tabs = MuiTabs(theme)
  const cards = MuiCard(theme)
  const input = MuiInput(theme)
  const tables = MuiTable(theme)
  const alerts = MuiAlerts(theme)
  const button = MuiButton(theme)
  const rating = MuiRating(theme)
  const avatars = MuiAvatar(theme)
  const divider = MuiDivider(theme)
  const dialog = MuiDialog(theme)
  const popover = MuiPopover(theme)
  const tooltip = MuiTooltip(theme)
  const backdrop = MuiBackdrop(theme)
  const snackbar = MuiSnackbar(theme)
  const switches = MuiSwitches(theme)
  const timeline = MuiTimeline(theme)
  const accordion = MuiAccordion(theme)
  const pagination = MuiPagination(theme)
  const dateTimePicker = MuiDateTimePicker(theme)

  return Object.assign(
    chip,
    list,
    menu,
    tabs,
    cards,
    input,
    alerts,
    button,
    dialog,
    rating,
    tables,
    avatars,
    divider,
    MuiLink,
    popover,
    tooltip,
    backdrop,
    MuiPaper,
    snackbar,
    switches,
    timeline,
    accordion,
    MuiSelect,
    pagination,
    MuiTypography,
    dateTimePicker,
    MuiToggleButton
  )
}

export default Overrides
