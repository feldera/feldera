// Footer for a dialog with tabs it contains the next and previous buttons. The

import IconLeftArrowAlt from '~icons/bx/left-arrow-alt'
import IconRightArrowAlt from '~icons/bx/right-arrow-alt'

// next button can turn into a submit button if we're on the last tab.
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'

const TabFooter = (props: { activeTab: string; setActiveTab: any; tabsArr: string[]; submitButton: JSX.Element }) => {
  const { activeTab, setActiveTab, tabsArr } = props

  const prevTab = tabsArr[tabsArr.indexOf(activeTab) - 1]
  const nextTab = tabsArr[tabsArr.indexOf(activeTab) + 1]
  const onLastTab = activeTab === tabsArr[tabsArr.length - 1]
  const onFirstTab = activeTab === tabsArr[0]

  const nextOrSaveButton = onLastTab ? (
    props.submitButton
  ) : (
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
