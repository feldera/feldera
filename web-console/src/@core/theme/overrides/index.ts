import { Theme } from '@mui/material/styles'

import MuiAccordion from './accordion'
import MuiAlerts from './alerts'
import MuiAvatar from './avatars'
import MuiBackdrop from './backdrop'
import MuiButton from './button'
// Overrides Imports
import MuiCard from './card'
import MuiChip from './chip'
import DataGrid from './dataGrid'
import MuiDateTimePicker from './dateTimePicker'
import MuiDialog from './dialog'
import MuiDivider from './divider'
import MuiInput from './input'
import MuiLink from './link'
import MuiList from './list'
import MuiMenu from './menu'
import MuiPagination from './pagination'
import MuiPaper from './paper'
import MuiPopover from './popover'
import MuiRating from './rating'
import MuiSelect from './select'
import MuiSnackbar from './snackbar'
import MuiSwitches from './switches'
import MuiTable from './table'
import MuiTabs from './tabs'
import MuiTimeline from './timeline'
import MuiToggleButton from './toggleButton'
import MuiTooltip from './tooltip'
import MuiTypography from './typography'

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
  const dataGrid = DataGrid(theme)

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
    MuiToggleButton,
    dataGrid
  )
}

export default Overrides
