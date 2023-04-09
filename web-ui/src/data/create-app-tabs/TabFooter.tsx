import Box from '@mui/material/Box'
import Button from '@mui/material/Button'

import { useSettings } from 'src/@core/hooks/useSettings'
import { Icon } from '@iconify/react'

const TabFooter = (props: { activeTab: string; setActiveTab: any; tabsArr: string[]; formId: string }) => {
  const { activeTab, setActiveTab, tabsArr, formId } = props

  const { settings } = useSettings()
  const { direction } = settings

  const nextArrow = direction === 'ltr' ? 'bx:right-arrow-alt' : 'bx:left-arrow-alt'
  const previousArrow = direction === 'ltr' ? 'bx:left-arrow-alt' : 'bx:right-arrow-alt'
  const prevTab = tabsArr[tabsArr.indexOf(activeTab) - 1]
  const nextTab = tabsArr[tabsArr.indexOf(activeTab) + 1]
  const onLastTab = activeTab === tabsArr[tabsArr.length - 1]
  const onFirstTab = activeTab === tabsArr[0]

  let nextOrSaveButton
  if (!onLastTab) {
    nextOrSaveButton = (
      <Button
        variant='contained'
        color='primary'
        endIcon={<Icon icon={nextArrow} />}
        onClick={() => {
          setActiveTab(nextTab)
        }}
      >
        Next
      </Button>
    )
  } else {
    nextOrSaveButton = (
      <Button variant='contained' color='success' endIcon={<Icon icon='bx:check' />} form={formId} type='submit'>
        Create
      </Button>
    )
  }

  return (
    <Box sx={{ mt: 4, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
      <Button
        variant='outlined'
        color='secondary'
        disabled={onFirstTab}
        onClick={() => setActiveTab(prevTab)}
        startIcon={<Icon icon={previousArrow} />}
      >
        Previous
      </Button>
      {nextOrSaveButton}
    </Box>
  )
}

export default TabFooter
