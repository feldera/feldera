// Footer for a dialog with tabs it contains the next and previous buttons. The

import IconCheck from '~icons/bx/check'
import IconLeftArrowAlt from '~icons/bx/left-arrow-alt'
import IconRightArrowAlt from '~icons/bx/right-arrow-alt'

// next button can turn into a submit button if we're on the last tab.
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'

const TabFooter = (props: { activeTab: string; setActiveTab: any; tabsArr: string[]; isUpdate: boolean }) => {
  const { activeTab, setActiveTab, tabsArr } = props

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
        endIcon={<IconRightArrowAlt />}
        onClick={() => {
          setActiveTab(nextTab)
        }}
      >
        Next
      </Button>
    )
  } else {
    nextOrSaveButton = (
      <Button variant='contained' color='success' endIcon={<IconCheck />} type='submit'>
        {props.isUpdate ? 'Update' : 'Create'}
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
        startIcon={<IconLeftArrowAlt />}
      >
        Previous
      </Button>
      {nextOrSaveButton}
    </Box>
  )
}

export default TabFooter
