import Dayjs from 'dayjs'
import duration from 'dayjs/plugin/duration'
import { applyAppShell } from './testAppShell'
import '../routes/layout.css'

Dayjs.extend(duration)

applyAppShell()
